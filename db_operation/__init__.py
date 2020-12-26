import pymysql
from pymysql.err import Error, OperationalError, InternalError, DataError
import os
from dotenv import load_dotenv

load_dotenv()


class TaskNotExist(DataError):
    pass


class WhoscallTest:
    TABLE_EXISTS_ERROR = 1050
    DUP_ENTRY = 1062
    DATA_TOO_LONG = 1406
    TASK_NOT_FOUND = 0
    LAST_INSERT_ID_NOT_FOUND = 1

    def __init__(self,  *args, **kwargs):
        self.tbname = kwargs['tbname']
        self.connection = pymysql.connect(
            user=os.getenv('DBUSER'),
            password=os.getenv('DBPASSWORD'),
            db=os.getenv('DBNAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )

    def create_table(self):
        sql = f"CREATE TABLE {self.tbname} ( \
                    id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \
                    name VARCHAR(15) NOT NULL, \
                    status BOOL NOT NULL DEFAULT 0 CHECK(`status` BETWEEN 0 AND 1) \
                );"
        with self.connection.cursor() as cursor:
            cursor.execute(sql)

    def close(self):
        self.connection.close()

    def list_tasks(self):
        with self.connection.cursor() as cursor:
            sql = f"SELECT * FROM {self.tbname}"
            cursor.execute(sql)

        return cursor.fetchall()

    def create_task_and_return_task(self, task_name):
        with self.connection.cursor() as cursor:
            sql = f"INSERT INTO {self.tbname} (name) VALUES (%s);"
            cursor.execute(sql, (task_name, ))
            sql = "SELECT LAST_INSERT_ID() as task_id;"
            cursor.execute(sql)
        task_id = cursor.fetchone().get('task_id')
        self.connection.commit()

        if not task_id:
            raise InternalError(self.LAST_INSERT_ID_NOT_FOUND, 'No last insert id')

        return self.get_task(task_id)

    def get_task(self, task_id):
        with self.connection.cursor() as cursor:
            sql = f"SELECT * FROM {self.tbname} WHERE id=%s"
            cursor.execute(sql, (task_id, ))

        return cursor.fetchone()

    def update_task(self, task_id, update_info):
        ori_record = self.get_task(task_id)
        if not ori_record:
            raise TaskNotExist(self.TASK_NOT_FOUND, 'Task not exist')
        print(ori_record)
        print(update_info)
        with self.connection.cursor() as cursor:
            sql = f"UPDATE {self.tbname} SET name=%s, status=%s WHERE id=%s;"
            cursor.execute(sql, (update_info.get('name', ori_record['name']), update_info.get('status', ori_record['status']), task_id,))

        self.connection.commit()
        return self.get_task(task_id)

    def delete_task(self, task_id):
        ori_record = self.get_task(task_id)
        if not ori_record:
            raise TaskNotExist(self.TASK_NOT_FOUND, 'Task not exist')
        with self.connection.cursor() as cursor:
            sql = f"DELETE FROM {self.tbname} WHERE id=%s;"
            cursor.execute(sql, (task_id,))

        self.connection.commit()

    def get_columns(self):
        with self.connection.cursor() as cursor:
            sql = f"SELECT * FROM {self.tbname} LIMIT 1;"
            cursor.execute(sql)
            columns = [i[0] for i in cursor.description]

        return columns
