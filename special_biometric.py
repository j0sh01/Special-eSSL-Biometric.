import local_config as config
import requests
import datetime
import json
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
import shelve
from zk import ZK, const

# Error messages and allowlist
EMPLOYEE_NOT_FOUND_ERROR_MESSAGE = "No Employee found for the given employee field value"
EMPLOYEE_INACTIVE_ERROR_MESSAGE = "Transactions cannot be created for an Inactive Employee"
DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE = "This employee already has a log with the same timestamp"
allowlisted_errors = [EMPLOYEE_NOT_FOUND_ERROR_MESSAGE, EMPLOYEE_INACTIVE_ERROR_MESSAGE, DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE]

if hasattr(config, 'allowed_exceptions'):
    allowlisted_errors_temp = []
    for error_number in config.allowed_exceptions:
        allowlisted_errors_temp.append(allowlisted_errors[error_number - 1])
    allowlisted_errors = allowlisted_errors_temp

device_punch_values_IN = getattr(config, 'device_punch_values_IN', [0, 4])
device_punch_values_OUT = getattr(config, 'device_punch_values_OUT', [1, 5])
ERPNEXT_VERSION = getattr(config, 'ERPNEXT_VERSION', 14)

# Setup logger and status
def setup_logger(name, log_file, level=logging.INFO, formatter=None):
    if not formatter:
        formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
    handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=50)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    return logger

if not os.path.exists(config.LOGS_DIRECTORY):
    os.makedirs(config.LOGS_DIRECTORY)
error_logger = setup_logger('error_logger', os.path.join(config.LOGS_DIRECTORY, 'error.log'), logging.ERROR)
info_logger = setup_logger('info_logger', os.path.join(config.LOGS_DIRECTORY, 'logs.log'))

# Utility functions
def _apply_function_to_key(obj, key, fn):
    obj[key] = fn(obj[key])
    return obj

def _safe_convert_date(datestring, pattern):
    try:
        return datetime.datetime.strptime(str(datestring), pattern)
    except:
        return None

def _safe_get_error_str(res):
    try:
        error_json = json.loads(res._content)
        if 'exc' in error_json:
            error_str = json.loads(error_json['exc'])[0]
        else:
            error_str = json.dumps(error_json)
    except:
        error_str = str(res.__dict__)
    return error_str

def get_dump_file_name_and_directory(device_id, device_ip):
    return os.path.join(config.LOGS_DIRECTORY, f"{device_id}_{device_ip.replace('.', '_')}_last_fetch_dump.json")

def get_last_line_from_file(file):
    line = None
    if not os.path.exists(file):
        return None
    if os.stat(file).st_size < 5000:
        with open(file, 'r') as f:
            for line in f:
                pass
    else:
        with open(file, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            line = f.readline().decode()
    return line

def main():
    status_file = os.path.join(config.LOGS_DIRECTORY, 'status')
    with shelve.open(status_file) as status:
        try:
            last_lift_off_timestamp = _safe_convert_date(status.get('lift_off_timestamp'), "%Y-%m-%d %H:%M:%S.%f")
            if (last_lift_off_timestamp and last_lift_off_timestamp < datetime.datetime.now() - datetime.timedelta(minutes=config.PULL_FREQUENCY)) or not last_lift_off_timestamp:
                status['lift_off_timestamp'] = str(datetime.datetime.now())
                info_logger.info("Cleared for lift off!")
                print("[INFO] Cleared for lift off!")
                for device in config.devices:
                    device_attendance_logs = None
                    info_logger.info(f"Processing Device: {device['device_id']}")
                    print(f"[INFO] Processing Device: {device['device_id']}")
                    dump_file = get_dump_file_name_and_directory(device['device_id'], device['ip'])
                    if os.path.exists(dump_file):
                        info_logger.error('Device Attendance Dump Found in Log Directory. Retrying with dumped data.')
                        print('[WARN] Device Attendance Dump Found. Retrying with dumped data.')
                        with open(dump_file, 'r') as f:
                            file_contents = f.read()
                            if file_contents:
                                device_attendance_logs = list(map(lambda x: _apply_function_to_key(x, 'timestamp', datetime.datetime.fromtimestamp), json.loads(file_contents)))
                    try:
                        pull_process_and_push_data(device, status, device_attendance_logs)
                        status[f"{device['device_id']}_push_timestamp"] = str(datetime.datetime.now())
                        if os.path.exists(dump_file):
                            os.remove(dump_file)
                        info_logger.info(f"Successfully processed Device: {device['device_id']}")
                        print(f"[INFO] Successfully processed Device: {device['device_id']}")
                    except Exception as e:
                        error_logger.exception('Exception when calling pull_process_and_push_data function for device' + json.dumps(device, default=str))
                        print(f"[ERROR] Exception for device {device['device_id']}: {e}")
                if hasattr(config, 'shift_type_device_mapping'):
                    update_shift_last_sync_timestamp(config.shift_type_device_mapping, status)
                status['mission_accomplished_timestamp'] = str(datetime.datetime.now())
                info_logger.info("Mission Accomplished!")
                print("[INFO] Mission Accomplished!")
        except Exception as e:
            error_logger.exception('Exception has occurred in the main function...')
            print(f"[ERROR] Exception in main: {e}")

def pull_process_and_push_data(device, status, device_attendance_logs=None):
    attendance_success_log_file = '_'.join(["attendance_success_log", device['device_id']])
    attendance_failed_log_file = '_'.join(["attendance_failed_log", device['device_id']])
    attendance_success_logger = setup_logger(attendance_success_log_file, os.path.join(config.LOGS_DIRECTORY, attendance_success_log_file) + '.log')
    attendance_failed_logger = setup_logger(attendance_failed_log_file, os.path.join(config.LOGS_DIRECTORY, attendance_failed_log_file) + '.log')
    if not device_attendance_logs:
        device_attendance_logs = get_all_attendance_from_device(device['ip'], status, device_id=device['device_id'], clear_from_device_on_fetch=device['clear_from_device_on_fetch'])
        if not device_attendance_logs:
            print(f"[WARN] No attendance logs fetched for device {device['device_id']}")
            return
    index_of_last = -1
    last_line = get_last_line_from_file(os.path.join(config.LOGS_DIRECTORY, attendance_success_log_file) + '.log')
    import_start_date = _safe_convert_date(config.IMPORT_START_DATE, "%Y%m%d")
    if last_line or import_start_date:
        last_user_id = None
        last_timestamp = None
        if last_line:
            last_user_id, last_timestamp = last_line.split("\t")[4:6]
            last_timestamp = datetime.datetime.fromtimestamp(float(last_timestamp))
        if import_start_date:
            if last_timestamp:
                if last_timestamp < import_start_date:
                    last_timestamp = import_start_date
                    last_user_id = None
            else:
                last_timestamp = import_start_date
        for i, x in enumerate(device_attendance_logs):
            if last_user_id and last_timestamp:
                if last_user_id == str(x['user_id']) and last_timestamp == x['timestamp']:
                    index_of_last = i
                    break
            elif last_timestamp:
                if x['timestamp'] >= last_timestamp:
                    index_of_last = i
                    break
    for device_attendance_log in device_attendance_logs[index_of_last + 1:]:
        punch_direction = device['punch_direction']
        if punch_direction == 'AUTO':
            if device_attendance_log['punch'] in device_punch_values_OUT:
                punch_direction = 'OUT'
            elif device_attendance_log['punch'] in device_punch_values_IN:
                punch_direction = 'IN'
            else:
                punch_direction = None
        erpnext_status_code, erpnext_message = send_to_erpnext(device_attendance_log['user_id'], device_attendance_log['timestamp'], device['device_id'], punch_direction)
        if erpnext_status_code == 200:
            attendance_success_logger.info("\t".join([erpnext_message, str(device_attendance_log['uid']), str(device_attendance_log['user_id']), str(device_attendance_log['timestamp'].timestamp()), str(device_attendance_log['punch']), str(device_attendance_log['status']), json.dumps(device_attendance_log, default=str)]))
            print(f"[SUCCESS] Synced log for user {device_attendance_log['user_id']} at {device_attendance_log['timestamp']}")
        else:
            attendance_failed_logger.error("\t".join([str(erpnext_status_code), str(device_attendance_log['uid']), str(device_attendance_log['user_id']), str(device_attendance_log['timestamp'].timestamp()), str(device_attendance_log['punch']), str(device_attendance_log['status']), json.dumps(device_attendance_log, default=str)]))
            print(f"[ERROR] Failed to sync log for user {device_attendance_log['user_id']} at {device_attendance_log['timestamp']}: {erpnext_status_code} {erpnext_message}")
            if not (any(error in erpnext_message for error in allowlisted_errors)):
                raise Exception('API Call to ERPNext Failed.')

def get_all_attendance_from_device(ip, status, port=4370, timeout=30, device_id=None, clear_from_device_on_fetch=False):
    zk = ZK(ip, port=port, timeout=timeout)
    conn = None
    attendances = []
    try:
        print(f"[INFO] Connecting to device {ip} ...")
        conn = zk.connect()
        x = conn.disable_device()
        info_logger.info("\t".join((ip, "Device Disable Attempted. Result:", str(x))))
        print(f"[INFO] Device {ip} disabled for attendance fetch.")
        attendances = conn.get_attendance()
        info_logger.info("\t".join((ip, "Attendances Fetched:", str(len(attendances)))))
        print(f"[INFO] Fetched {len(attendances)} attendances from device {ip}.")
        if device_id:
            status[f'{device_id}_push_timestamp'] = None
            status[f'{device_id}_pull_timestamp'] = str(datetime.datetime.now())
        if len(attendances):
            dump_file_name = get_dump_file_name_and_directory(device_id, ip)
            with open(dump_file_name, 'w+') as f:
                f.write(json.dumps(list(map(lambda x: x.__dict__, attendances)), default=datetime.datetime.timestamp))
            if clear_from_device_on_fetch:
                x = conn.clear_attendance()
                info_logger.info("\t".join((ip, "Attendance Clear Attempted. Result:", str(x))))
                print(f"[INFO] Cleared attendance logs from device {ip} after fetch.")
        x = conn.enable_device()
        info_logger.info("\t".join((ip, "Device Enable Attempted. Result:", str(x))))
        print(f"[INFO] Device {ip} enabled after attendance fetch.")
    except Exception as e:
        error_logger.exception(str(ip)+' exception when fetching from device...')
        print(f"[ERROR] Exception when fetching from device {ip}: {e}")
        raise Exception('Device fetch failed.')
    finally:
        if conn:
            conn.disconnect()
    return list(map(lambda x: x.__dict__, attendances))

def send_to_erpnext(employee_field_value, timestamp, device_id=None, log_type=None):
    endpoint_app = "hrms" if ERPNEXT_VERSION > 13 else "erpnext"
    url = f"{config.ERPNEXT_URL}/api/method/{endpoint_app}.hr.doctype.employee_checkin.employee_checkin.add_log_based_on_employee_field"
    headers = {
        'Authorization': "token "+ config.ERPNEXT_API_KEY + ":" + config.ERPNEXT_API_SECRET,
        'Accept': 'application/json'
    }
    data = {
        'employee_field_value' : employee_field_value,
        'timestamp' : timestamp.__str__(),
        'device_id' : device_id,
        'log_type' : log_type
    }
    try:
        response = requests.request("POST", url, headers=headers, json=data)
        if response.status_code == 200:
            return 200, json.loads(response._content)['message']['name']
        else:
            error_str = _safe_get_error_str(response)
            error_logger.error('\t'.join(['Error during ERPNext API Call.', str(employee_field_value), str(timestamp.timestamp()), str(device_id), str(log_type), error_str]))
            print(f"[ERROR] ERPNext API error for user {employee_field_value}: {error_str}")
            return response.status_code, error_str
    except Exception as e:
        error_logger.exception(f"Exception during ERPNext API call: {e}")
        print(f"[ERROR] Exception during ERPNext API call: {e}")
        return 500, str(e)

def update_shift_last_sync_timestamp(shift_type_device_mapping, status):
    for shift_type_device_map in shift_type_device_mapping:
        all_devices_pushed = True
        pull_timestamp_array = []
        for device_id in shift_type_device_map['related_device_id']:
            if not status.get(f'{device_id}_push_timestamp'):
                all_devices_pushed = False
                break
            pull_timestamp_array.append(_safe_convert_date(status.get(f'{device_id}_pull_timestamp'), "%Y-%m-%d %H:%M:%S.%f"))
        if all_devices_pushed and pull_timestamp_array:
            min_pull_timestamp = min(pull_timestamp_array)
            if isinstance(shift_type_device_map['shift_type_name'], str):
                shift_type_device_map['shift_type_name'] = [shift_type_device_map['shift_type_name']]
            for shift in shift_type_device_map['shift_type_name']:
                try:
                    sync_current_timestamp = _safe_convert_date(status.get(f'{shift}_sync_timestamp'), "%Y-%m-%d %H:%M:%S.%f")
                    if (sync_current_timestamp and min_pull_timestamp > sync_current_timestamp) or (min_pull_timestamp and not sync_current_timestamp):
                        response_code = send_shift_sync_to_erpnext(shift, min_pull_timestamp)
                        if response_code == 200:
                            status[f'{shift}_sync_timestamp'] = str(min_pull_timestamp)
                except Exception as e:
                    error_logger.exception('Exception in update_shift_last_sync_timestamp, for shift:'+shift)
                    print(f"[ERROR] Exception updating shift sync timestamp for {shift}: {e}")

def send_shift_sync_to_erpnext(shift_type_name, sync_timestamp):
    url = config.ERPNEXT_URL + "/api/resource/Shift Type/" + shift_type_name
    headers = {
        'Authorization': "token "+ config.ERPNEXT_API_KEY + ":" + config.ERPNEXT_API_SECRET,
        'Accept': 'application/json'
    }
    data = {
        "last_sync_of_checkin" : str(sync_timestamp)
    }
    try:
        response = requests.request("PUT", url, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            info_logger.info("\t".join(['Shift Type last_sync_of_checkin Updated', str(shift_type_name), str(sync_timestamp.timestamp())]))
            print(f"[INFO] Shift Type {shift_type_name} last_sync_of_checkin updated to {sync_timestamp}")
        else:
            error_str = _safe_get_error_str(response)
            error_logger.error('\t'.join(['Error during ERPNext Shift Type API Call.', str(shift_type_name), str(sync_timestamp.timestamp()), error_str]))
            print(f"[ERROR] ERPNext Shift Type API error for {shift_type_name}: {error_str}")
        return response.status_code
    except Exception as e:
        error_logger.exception(f"Exception updating last_sync_of_checkin in Shift Type {shift_type_name}: {e}")
        print(f"[ERROR] Exception updating last_sync_of_checkin in Shift Type {shift_type_name}: {e}")
        return 500

def infinite_loop(sleep_time=15):
    print("[INFO] Special Biometric Service Running...")
    while True:
        try:
            main()
            time.sleep(sleep_time)
        except BaseException as e:
            print(f"[FATAL] {e}")
            error_logger.exception(f"[FATAL] {e}")

if __name__ == "__main__":
    infinite_loop()
