from db_operation import WhoscallTest


def is_valid_json(request):
    if not request.is_json or not request.data:
        return False

    try:
        data = request.json
    except:
        return False

    return True


def is_valid_task_name(request, task_name_limit):
    data = request.json
    task_name = data.get('name', None)

    if not task_name:
        return False
    if len(task_name) >= task_name_limit:
        return False

    return True


def is_valid_task_record(request):
    task_name = request.json.get('name', None)
    task_status = request.json.get('status', None)

    if not task_name or task_status not in (0, 1):
        return False

    return True


def format_db_err(err_code):
    status_code = 500
    err = {}
    if err_code == WhoscallTest.DUP_ENTRY:
        status_code = 409
        err['err_msg'] = 'This task exists'
    elif err_code == WhoscallTest.DATA_TOO_LONG:
        status_code = 400
        err['err_msg'] = 'Task name length too long'
    elif err_code == WhoscallTest.TASK_NOT_FOUND:
        err['err_msg'] = 'Task not exist'
    elif err_code == WhoscallTest.LAST_INSERT_ID_NOT_FOUND:
        err['err_msg'] = 'No last insert id'
    else:
        err['err_msg'] = 'Something wrong'

    return err, status_code


def format_sys_err(unknown_err):
    status_code = 500
    err = {
        'err_msg': 'Something wrong'
    }

    return err, status_code
