from typing import Tuple

import pandas as pd


class SpatialDataSubmissionFormError(Exception):
    pass


class Report:
    def __init__(self, excel_path, sheet_name="DATASET DETAILS"):
        self.source = excel_path
        self.sheet_name = sheet_name

        self.df = self.to_dataframe(self.sheet_name)
        self.feature_class_name, self.feature_shape, self.feature_type = self.report_details()

    def to_dataframe(self, sheet_name):
        df = pd.read_excel(
            io=self.source,
            sheet_name=sheet_name,
            index_col=0
        )
        df = df.where(pd.notnull(df), None)  # Remove NaN values with None
        df = df[pd.notnull(df.index)]  # Remove blank lines from index
        return df

    def report_details(self):
        df_feature_details = self.df.iloc[0:3, 0:1]  # first col in excel is index - col 0 in df is second col in excel
        df_feature_details = df_feature_details.where(pd.notnull(df_feature_details), None)  # Convert nan to None

        df_feature_details = df_feature_details.T  # Transpose
        df_feature_details.columns = [x.strip(":") for x in df_feature_details.columns]

        # Check for 'Feature Class Name' or 'Data Source Name' column and get the value
        if 'Feature Class Name' in df_feature_details.columns:
            feature_class_name = df_feature_details['Feature Class Name'].values[0]

        elif 'Data Source Name' in df_feature_details.columns:
            feature_class_name = df_feature_details['Data Source Name'].values[0]

        else:
            raise ValueError("Neither 'Feature Class Name' nor 'Data Source Name' column found in the DataFrame")

        shape_type = df_feature_details["Shape Type"].values[0] or "Enterprise Geodatabase Table"
        feature_type = df_feature_details["Feature Type"].values[0]

        return [feature_class_name, shape_type, feature_type]


class SDSFMetaData:

    def __init__(self, excel_path, sheet_name="SDSF"):
        self.source = excel_path

        self.df = pd.read_excel(self.source, sheet_name=sheet_name)

        self.name = self.get_name()

        self.description = self.get_description()
        self.summary = self.get_summary()
        self.tags = self.get_tags()

        self.limitations = self.get_limitations()

    def __repr__(self):
        return self.name

    def get_description(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Description:'].index[0]
        desc = self.df.iloc[header_idx + 1, 0]  # row, col
        return desc

    def get_summary(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Purpose:'].index[0]
        desc = self.df.iloc[header_idx + 1, 0]  # row, col
        return desc

    def get_tags(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Tags:'].index[0]
        tags = self.df.iloc[header_idx + 1, 0]  # row, col
        return tags

    def get_limitations(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Notes or Disclaimers:'].index[0]
        limits = self.df.iloc[header_idx + 1, 0]  # row, col
        return limits

    def get_name(self):
        header_idx = self.df.loc[self.df['Spatial Data Submission Form'] == 'Dataset Name:'].index[0]
        name = self.df.iloc[header_idx, 1]  # row, col
        return f"METADATA: {name}"


class FieldsReport(Report):
    
    last_field_name = "GLOBALID"
    def __init__(self, excel_path, sheet_name="DATASET DETAILS"):
        super().__init__(excel_path, sheet_name)
        self.field_details = self.field_info()

        if "Subtype Field" in [x for x in self.field_details.columns]:
            self.subtype_fields = self.subtype_info()

    def subtype_info(self):
        fields_df = self.field_details

        if "Subtype Field" not in [x for x in fields_df.columns]:
            return ()

        subtype_field_df = fields_df[fields_df["Subtype Field"].notnull()]

        if not subtype_field_df.empty:
            subtype_fields = (x for x in subtype_field_df["Field Name"])
            return subtype_fields

    def field_info(self):
        df_index_values = self.df.index.values.tolist()

        if self.feature_type.upper() == "FEATURE CLASS":
            
            FieldsReport.last_field_name = "SHAPE_Length"

            if self.feature_shape.upper() == "POLYGON":
                if "SHAPE_AREA" not in [str(x).upper() for x in df_index_values] or "SHAPE_LENGTH" not in [str(x).upper() for x in df_index_values]:
                    raise IndexError(f"ERROR: SDSF needs to have SHAPE_AREA and SHAPE_LENGTH fields.")

            elif self.feature_shape.upper() == "LINE":
                if "SHAPE_LENGTH" not in [str(x).upper() for x in df_index_values]:
                    raise IndexError(f"ERROR: SDSF needs to have a SHAPE_LENGTH field.")

            elif self.feature_shape.upper() == "NOT APPLICABLE" or self.feature_shape.upper() == "POINT":
                FieldsReport.last_field_name = "GLOBALID"

        df_field_details = self.df.loc["Alias":FieldsReport.last_field_name]

        df_field_details.reset_index(inplace=True)

        df_field_details.columns = df_field_details.iloc[0]  # Set DataFrame columns as first row
        columns = [x for x in df_field_details.columns if x]

        df_field_details = df_field_details.loc[:, columns]  # Limit columns to columns list

        df_field_details = df_field_details.iloc[1:]  # Ignore columns row as a data row

        return df_field_details

    def domain_fields(self) -> dict:
        domain_fields = self.field_details[["Field Name", "Domain", "Field Type"]][
            ~self.field_details["Domain"].isnull()]

        domain_fields_info = domain_fields.to_dict("records")

        return domain_fields_info


class DomainsReport(Report):
    
    domains_section_header = "Fields with associated codes(values) and descriptions"
    
    def __init__(self, excel_path, subtype_field=(), sheet_name="DATASET DETAILS"):
        super().__init__(excel_path, sheet_name)

        self.subtype_field = subtype_field

        self.domain_df = pd.DataFrame()

        # Populate domain information on initialization so attributes
        # are immediately available for consuming modules such as
        # SpatialDataSubmissionForms.main
        self.domain_names, self.domain_data = self.domain_info()

    def domain_info(self) -> Tuple[list, dict]:
        """Parse domain information from the SDSF worksheet.

        Returns a tuple of ``(domain_names, domain_data)`` where
        ``domain_names`` is a list of domain names found in the sheet and
        ``domain_data`` is a dictionary mapping those domain names to
        :class:`pandas.DataFrame` objects of coded values.
        """

        domain_dataframes = dict()

        # Get domain info from main spreadsheet - Starts at first row after "Common Attribute Values for Fields"
        self.domain_df = self.df.loc[DomainsReport.domains_section_header:].iloc[1:]

        # Check index for a mis-named SourceAccuracy field
        df_index = self.domain_df.index.tolist()
        for count, value in enumerate(df_index):
            if type(value) == str and "SourceAccuracy" in value:
                df_index[count] = "SourceAccuracy"

        self.domain_df.index = df_index

        # Create json structure for domains
        index_data = dict()

        # Iterate through index to domains
        for count, index_value in enumerate(self.domain_df.index):

            if str(index_value).upper() == "CODE":
                
                prev_row = count - 1
                domain_field_name = df_index[prev_row]  # TODO: This is not the field name.

                # domain_name = df_index[prev_row]  # The value above "Code"
                domain_name = self.domain_df.iloc[prev_row, 0]

                row_index_start = self.domain_df.index.tolist().index(domain_field_name)  # Domain name will precede row index with value of Code

                index_data[domain_name] = {"start_index": row_index_start, "domain_field_name": domain_field_name}

        domain_names = list(index_data.keys())

        if domain_names:
            last_domain = domain_names[-1]
    
            # Check that no spaces are in domain - make sure SDSF is filled out correctly
            bad_domain_names = list()
    
            for domain_name in domain_names:
                if domain_name.count(" ") > 0:
                    bad_domain_names.append(domain_name)
    
            if bad_domain_names:
                error_message = f"\n\tDomain filled out incorrectly. " \
                                f"Double check domain names, '{', '.join(bad_domain_names)}' and " \
                                f"ensure no spaces are present."
                raise SpatialDataSubmissionFormError(error_message)
    
            for count, current_domain_name in enumerate(domain_names):
                next_domain = None
                
                if current_domain_name != last_domain:
                    next_domain = domain_names[count + 1]
                
                domain_field = index_data[current_domain_name]['domain_field_name']
                
                if next_domain:
                    next_domain_field = index_data[next_domain]['domain_field_name']

                    domain_df = self.domain_df.loc[domain_field: next_domain_field]

                else:
                    domain_df = self.domain_df.loc[domain_field:]

                domain_df.reset_index(inplace=True)  # Adds current index as first column
                domain_df.columns = domain_df.iloc[1]  # Set first column as df header
    
                domain_field = domain_df.iloc[0, 0]
                domain_name = domain_df.iloc[0, 1]
    
                if next_domain:
                    # domain name, domain field, subtype code
                    domain_df = domain_df.iloc[2:-1, :2]  # Only select 2nd to 2nd last row and first two columns
    
                else:
                    domain_df = domain_df.iloc[2:, :2]  # Only select 2nd to 2nd last row and first two columns
    
                domain_df.dropna(inplace=True)
    
                # Clean
                # Remove any domain dataframes with empty rows
                num_df_rows = len(domain_df.index)
                if not num_df_rows == 0:
                    domain_dataframes[current_domain_name] = domain_df


        return domain_names, domain_dataframes
