from lib.change_db import ChangeDb
 
if __name__ == '__main__':
    
    x = ChangeDb(
        driver = "mysql",
        username = "my_user",
        password = "pmy_pass",
        host = "my_machine.us-east-1.rds.amazonaws.com",
        port = 3306,
        schema = "test"
    )
    
    x.update_or_insert(
        path = '/home/username/Downloads/updated_data.xlsx',
        table_name = 'test_table',
        type = 'xlsx'
    )