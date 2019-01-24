import clean_raw_data
import validate_clean_data
import pandas
import csv
import utilities

def test_clean_col_country_name():
    """
    Test the cleaning for column: "country_name"
    """
    assert clean_raw_data.clean_col_country_name('NA') == 'NA'

def test_clean_col_install_year():
    """
    Test the cleaning for column: "install_year"
    """
    assert clean_raw_data.clean_col_install_year('2001') == "2001"
    
def test_clean_col_fecal_coliform_presence():
    """
    Test the cleaning for column: "fecal_coliform_presence"
    """
    #assert pandas.isna(clean_raw_data.clean_col_fecal_coliform_presence('junk'))
    #assert clean_raw_data.clean_col_fecal_coliform_presence('Presence') == 'Presence'
    #assert clean_raw_data.clean_col_fecal_coliform_presence('Absence') == 'Absence'

def test_clean_col_adm1():
    """
    Test the cleaning for column: "adm1"
    """
    assert clean_raw_data.clean_col_adm1('singapore') == 'SINGAPORE'
    assert clean_raw_data.clean_col_adm1(' Singapore ') == 'SINGAPORE'

if __name__ == '__main__':

    column_names = utilities.get_column_names('./wpdx_sample_data.csv')

    print('Executing unit tests')
    for column_name in column_names:
            unit_test_name = 'test_clean_col_' + column_name
            if unit_test_name in globals():
                #print('Executing unit test ' + unit_test_name)
                globals()[unit_test_name]()
    
    print('Cleaning up columns')
    clean_raw_data.clean_columns('wpdx_sample_data.csv', 'cleaned_wpdx_sample_data.csv')

    print('Validating cleaned file')
    if validate_clean_data.validate_clean_data('./cleaned_wpdx_sample_data.csv', './validation_config.json') == 0 :
        print('Cleaned Data validated')
    else :
        #Run analytics and generate summary report
        print('Data needs some more cleanup')