from flask import Flask, request, make_response, jsonify
from werkzeug.exceptions import BadRequest
from yaml import safe_load
import json
import signal
import sys
import os
from dotenv import load_dotenv
import pymysql
from pymysql.err import DatabaseError, IntegrityError, DataError
import logging
from logging.config import dictConfig
from db_operation import WhoscallTest, TaskNotExist
from handlers import is_valid_json, is_valid_task_name, is_valid_task_record, format_db_err, format_sys_err


load_dotenv()

with open('logging.yaml', 'r') as f:
    config = safe_load(f)
    dictConfig(config)

logger = logging.getLogger(__name__)

mydb = 'none'

app = Flask(__name__)


def quit_handler(signal, frame):
    print('Quit app')
    if mydb:
        mydb.close()
    sys.exit(0)


signal.signal(signal.SIGINT, quit_handler)


@app.route('/tasks/test', methods=['GET'])
def test_tasks():
    return jsonify({'result': 'Hello'})

@app.route('/tasks', methods=['GET'])
def list_tasks():
    try:
        result = mydb.list_tasks()
    except DatabaseError as db_err:
        logger.error(db_err.args)
    except Exception:
        logger.error('', exc_info=True)

    return jsonify({'result': result})


@app.route('/task', methods=['POST'])
def create_task():
    if not is_valid_json(request):
        return ({
            'err_msg': 'Not a valid JSON object',
        }, 400)

    task_name_limit = 15
    if not is_valid_task_name(request, task_name_limit):
        return ({
            'err_msg': f'Not a valid task name. Require `name` field, and length must be smaller or equal than {task_name_limit}',
        }, 400)

    task_name = request.json.get('name', None)

    result = {}
    err = {}
    try:
        task = mydb.create_task_and_return_task(task_name)
    except DatabaseError as db_err:
        logger.error(db_err.args)
        err, status_code = format_db_err(db_err.args[0])
    except Exception as unknown_err:
        logger.error('', exc_info=True)
        err, status_code = format_sys_err(unknown_err)
    else:
        status_code = 201
        result['result'] = task

    return dict(result, **err), status_code


@app.route('/task/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    if not is_valid_json(request):
        return ({
            'err_msg': 'Not a valid JSON object',
        }, 400)

    if not is_valid_task_record(request):
        return ({
            'err_msg': 'Not a valid task record',
        }, 400)

    result = {}
    err = {}
    try:
        task = mydb.update_task(task_id, request.json)
    except (DatabaseError, TaskNotExist) as db_err:
        logger.error(db_err.args)
        err, status_code = format_db_err(db_err.args[0])
    except Exception as unknown_err:
        logger.error('', exc_info=True)
        err, status_code = format_sys_err(unknown_err)
    else:
        status_code = 200
        result['result'] = task

    return dict(result, **err), status_code


@app.route('/task/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    result = {}
    err = {}
    try:
        mydb.delete_task(task_id)
    except (DatabaseError, TaskNotExist) as db_err:
        logger.error(db_err.args)
        err, status_code = format_db_err(db_err.args[0])
    except Exception as unknown_err:
        logger.error('', exc_info=True)
        err, status_code = format_sys_err(unknown_err)
    else:
        status_code = 200
        result['result'] = 'ok'

    return dict(result, **err), status_code

try:
    mydb = WhoscallTest(tbname='Tasks')
    mydb.create_table()
except DatabaseError as e:
    err_code, err_msg = e.args[0], e.args[1]
    logger.error(e.args)
    if err_code != WhoscallTest.TABLE_EXISTS_ERROR:
        quit_handler('', '')
finally:
    logger.info('Successfully connect to db')

if __name__ == '__main__':
    app.run()
