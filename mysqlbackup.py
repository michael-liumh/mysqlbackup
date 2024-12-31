# !/usr/bin/env python3
# -*- coding:utf8 -*-
# Author:           Michael Liu
# Created on:       2024-12-26
import configargparse
import getpass
import sys
import platform
import time
import delegator
import psutil
from datetime import datetime
from loguru import logger
from threading import Thread
from pathlib import Path

py_file_path = Path(sys.argv[0])
py_file_pre = py_file_path.parts[-1].replace('.py', '')
log_file = py_file_path.parent / 'logs' / f'{py_file_pre}.log'
log_file.parent.mkdir(exist_ok=True, parents=True)
logger.add(log_file, rotation='100MB', colorize=True, retention=10, compression='zip')


def parse_mysql_args():
    """Parse args to connect to MySQL"""

    if platform.system() == "Windows":
        config_file_parser_class = configargparse.DefaultConfigFileParser
        default_config_files = ['config.ini', 'conf.d/*.ini']
    else:
        config_file_parser_class = configargparse.YAMLConfigFileParser
        default_config_files = [
            'config.yaml', 'conf.d/*.yaml',
            'config.yml', 'conf.d/*.yml'
        ]  # 可以设置更多路径

    port = 3306
    half_cpus = psutil.cpu_count() // 2
    threads = half_cpus if half_cpus > 4 else 3

    parser = configargparse.ArgumentParser(
        description='Parse Args', add_help=False,
        formatter_class=configargparse.ArgumentDefaultsHelpFormatter,
        config_file_parser_class=config_file_parser_class,
        default_config_files=default_config_files
    )
    parser.add_argument('--help', dest='help', action='store_true', default=False,
                        help='help information')
    parser.add_argument('-sc', '--script-config', is_config_file=True,
                        help='script config file path')

    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', env_var='MYSQL_HOST', dest='host',
                                 type=str, default='localhost',
                                 help='MySQL host for backup.')
    connect_setting.add_argument('-P', '--port', env_var='MYSQL_PORT', dest='port',
                                 type=int, default=port,
                                 help='MySQL port for backup.')
    connect_setting.add_argument('-S', '--socket', dest='socket', type=str,
                                 help='MySQL socket for backup.')
    connect_setting.add_argument('-u', '--user', env_var='MYSQL_USER', dest='user',
                                 type=str, default='root',
                                 help='MySQL User for backup.')
    connect_setting.add_argument('-p', '--password', env_var='MYSQL_PWD', dest='password',
                                 type=str, nargs='*', default='',
                                 help='MySQL Password for backup.')
    connect_setting.add_argument('--no-pass', dest='no_pass', action='store_true', default=False,
                                 help='No password')
    connect_setting.add_argument('-G', '--login-path', dest='login_path', type=str,
                                 help='MySQL login path for backup.')

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='Filter MySQL database for backup.')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='Filter MySQL database for backup.')

    config = parser.add_argument_group('config setting')
    config.add_argument('-mc', '--mysql-config', dest='config', type=str,
                        help='MySQL config.')

    backup = parser.add_argument_group('backup setting')
    backup.add_argument('--tool', dest='tool', type=str,
                        choices=[
                            'dump', 'mysqldump',
                            'pump', 'mysqlpump',
                            'xbk', 'xtrabackup',
                            'dumper', 'mydumper'
                        ],
                        help="Choice a backup tool.")
    backup.add_argument('--base-dir', dest='base_dir', type=str,
                        help="Base dir for get backup command if set.")

    backup.add_argument('--backup-dir', dest='backup_dir', type=str, default=f'/data/backups/mysql{port}',
                        help='Backup dir for save result.')
    backup.add_argument('--backup-file', dest='backup_file', type=str, default=f'',
                        help='Backup file for save result. '
                             '(default: <host_ip>_<port>_<tool>_<backup_time>.lz4|xb|stream)')
    backup.add_argument('-l', '--backup-log', dest='backup_log', type=str, default='process_mysql_backup.log',
                        help='Log for record backup process. ')

    backup.add_argument('--extra', dest='extra', type=str, nargs='*',
                        help="Set extra args for backup tool.")
    backup.add_argument('--inc', '--incremental', dest='incremental', action='store_true', default=False,
                        help='If set tool to xbk and xtrabackup_checkpoints file exists in history dir, '
                             'we will start incremental backup from last backup history. '
                             'Otherwise, we start a full backup')

    backup.add_argument('--just-insert', dest='just_insert', action='store_true', default=False,
                        help='Use for mysqldump and mysqlpump.')
    backup.add_argument('--no-data', dest='no_data', action='store_true', default=False,
                        help='Use for mysqldump and mysqlpump.')
    backup.add_argument('--threads', dest='threads', type=int, default=threads,
                        help='Use for mysqlpump, xtrabackup and mydumper. (default: <half cpus>) ')

    other = parser.add_argument_group('other setting')
    other.add_argument('--debug', dest='debug', action='store_true', default=False,
                       help='Print command during process running.')
    other.add_argument('--reset', dest='reset', action='store_true', default=False,
                       help='Reset login path.')
    return parser


def parse_mysql_args_from_command_line(args, parser=None):
    if parser is None:
        parser = parse_mysql_args()
    args = parser.parse_args(args)

    need_print_help = False if args.tool else True
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)

    if not args.tool:
        logger.error('the following arguments are required: --tool')
        sys.exit(1)

    if not args.password and not args.no_pass and not args.login_path:
        args.password = getpass.getpass()
    elif args.no_pass:
        args.password = ''
    elif not args.login_path:
        args.password = args.password[0]

    if args.tool in ['xbk', 'xtrabackup']:
        args.tool = 'xtrabackup'
    elif args.tool in ['dump', 'mysqldump']:
        args.tool = 'mysqldump'
    elif args.tool in ['pump', 'mysqlpump']:
        args.tool = 'mysqlpump'
    else:
        args.tool = 'mydumper'
        if args.login_path:
            logger.error('mydumper does not support for args --login-path')
            sys.exit(1)

    if args.tool == 'mydumper' and args.tables:
        logger.warning(f'--tables must set the db_name in front of tb_name. '
                       f'for example: use --tables test.t1, '
                       f'not --databases test --tables t1')

    if args.base_dir:
        args.base_dir = Path(args.base_dir)
        if not args.base_dir.exists():
            logger.error(f'Base dir [{args.base_dir.name}] does not exists.')
            sys.exit(1)

    args.tool = Path(args.tool) if not args.base_dir else args.base_dir / args.tool

    args.backup_dir = Path(args.backup_dir)
    args.backup_dir.mkdir(parents=True, exist_ok=True)

    args.backup_log = py_file_path.parent / 'logs' / args.backup_log

    if not args.backup_file:
        get_ip_command = '''
            ip address show $(ip r | awk '/default/{print $5}') | grep inet | 
            grep -v inet6 | awk -F "[ |/]+" '{print $3}'
        '''.replace('\n', '')
        resp = delegator.run(get_ip_command)
        host = resp.out.replace('\n', '') if resp.return_code == 0 else args.host

        datetime_format = '%Y%m%d_%H%M%S'
        datetime_str = datetime.now().strftime(datetime_format)

        if args.tool.name == 'xtrabackup':
            backup_file_suffix = 'fullback.xb'
            if args.incremental:
                if (args.backup_dir.absolute() / 'history' / 'xtrabackup_checkpoints').exists():
                    backup_file_suffix = 'incremental.xb'
                else:
                    logger.warning(f'last fullback does not exists, ignore args: --incremental')
        elif args.tool.name == 'mydumper':
            backup_file_suffix = 'stream'
        else:
            backup_file_suffix = 'sql.lz4'
        args.backup_file = args.backup_dir / (f'{host.replace(".", "_")}_{args.port}_{args.tool.name}_'
                                              f'{datetime_str}.{backup_file_suffix}')
    else:
        args.backup_file = Path(args.backup_file)

    if args.extra is None:
        args.extra = []
    return args


def add_extra_args(args):
    args.extra = "".join(args.extra)

    if args.tool.name in ['mysqldump', 'mysqlpump', 'mydumper']:
        if args.just_insert and args.tool.name in ['mysqldump', 'mysqlpump']:
            args.extra += f' --skip-add-drop-table --skip-add-locks --no-create-info'
        if args.no_data:
            if args.tool.name == 'mysqldump':
                args.extra += (f' --no-data --skip-lock-tables --skip-add-drop-database --skip-add-drop-table '
                               f'--skip-add-drop-trigger')
            elif args.tool.name == 'mysqlpump':
                args.extra += f' --skip-dump-rows'
            else:
                args.extra += f' --no-data --no-locks'
    return


def get_connect_args(args):
    connect_args = ''
    if args.config:
        connect_args += f' --defaults-file={args.config}'
    if args.socket:
        connect_args += f' --socket={args.socket}'

    if args.login_path:
        config_login_path(args)
        connect_args += f' --login-path={args.login_path}'
    else:
        connect_args += f' --host={args.host} --port={args.port} --user={args.user}'
    return connect_args


def check_hung(args):
    connect_args = get_connect_args(args)
    # PS: do not need to add condition: state='Waiting for table metadata lock',
    # because that thread does not belong backup process
    c_command = f'''
            mysql {connect_args} {args.extra} 
            --skip-column-names -e "select id from information_schema.processlist 
            where info='FLUSH TABLES WITH READ LOCK' and user='{args.user}';"
        '''.replace('\n', ' ')
    kill_command = '''
        mysql {connect_args} {extra} --skip-column-names -e "kill {thread_id};" 
    '''.replace('\n', ' ')
    i = 1
    if args.debug:
        logger.debug(c_command)
    while i < 180:
        if args.debug:
            logger.debug(f'Check hang {i} times.')
            logger.debug(c_command)
        resp = delegator.run(c_command, env={'MYSQL_PWD': args.password})
        if resp.return_code == 0 and i > 3:
            thread_ids = str(resp.out).split('\n')
            thread_ids.remove('')
            if thread_ids:
                logger.info(f'Found hang thread: [{"|".join(thread_ids)}], '
                            f'now killing it.')

            for t_id in thread_ids:
                kill_command_fill = kill_command.format(
                    connect_args=connect_args,
                    extra=args.extra,
                    thread_id=t_id
                )
                if args.debug:
                    logger.debug(kill_command_fill)
                resp = delegator.run(kill_command_fill, env={'MYSQL_PWD': args.password})
                if resp.return_code == 0:
                    logger.info(f'Successfully kill thread: {t_id}')
                else:
                    logger.error(f'Failed to kill thread: {t_id}')
        elif resp.return_code != 0:
            logger.error(f'Failed to check hang: {resp.err}')
            sys.exit(1)
        i += 1
        time.sleep(1)
    return


def check_command(command):
    if platform.system() == "Windows":
        c_command = f'where {command}'
    else:
        c_command = f'which {command}'

    resp = delegator.run(c_command)
    if resp.return_code != 0:
        logger.error(f'Could not find command: {command}')
        sys.exit(1)
    return


def pre_backup(args):
    # check command
    check_command(args.tool.name)
    check_command('lz4')

    # check backup process if exists
    connect_args = get_connect_args(args)
    program = command = f'{args.tool.name} {connect_args} {args.extra}'
    if platform.system() == "Windows":
        command = f"tasklist -v | findstr %{program}%"
    else:
        command = f"ps -ef | grep -v grep | grep -E '{program}"

    if args.debug:
        logger.debug(command)

    resp = delegator.run(command)
    if resp.return_code == 0:
        logger.error(f'Another backup process is running: {resp.out}')
        sys.exit(1)

    # check connect
    connect_args = get_connect_args(args)
    command = f"""
        mysql {connect_args} {args.extra} --skip-column-names 
        -e "select count(concat(table_schema, '.', table_name)) from information_schema.tables 
        where table_schema not in ('sys','information_schema','performance_schema');"
    """.replace('\n', ' ')
    map(lambda x: command.replace(x, ' '), ('\n', '  ', '\t'))
    if args.debug:
        logger.debug(command)
    resp = delegator.run(command, env={'MYSQL_PWD': args.password})
    if resp.return_code != 0:
        logger.error(f'Could not connect to mysql server: {args.host}:{args.port}. '
                     f'Got error: {resp.err}')
        sys.exit(1)
    return


def add_filter_args(args):
    filter_args = ''
    if args.databases:
        if args.tool.name == 'mysqldump':
            filter_args += f' --databases {" ".join(args.databases)}'
        elif args.tool.name == 'mysqlpump':
            filter_args += f' --include-databases {" ".join(args.databases)}'
        elif args.tool.name == 'mydumper':
            filter_args += " --regex '" + "\\.|".join(args.databases) + "\\.'"
        else:
            logger.error(f'We dont suggest use xtrabackup to backup specify databases or tables, '
                         f'please ues other tool for doing this. ignore these options')
            sys.exit(1)
    if args.tables:
        if args.tool.name == 'mysqldump':
            filter_args += f' --tables {" ".join(args.tables)}'
        elif args.tool.name == 'mysqlpump':
            filter_args += f' --include-tables {" ".join(args.tables)}'
        elif args.tool.name == 'mydumper':
            filter_args += f' --tables-list {",".join(args.tables)}'
        else:
            logger.error(f'We dont suggest use xtrabackup to backup specify databases or tables, '
                         f'please ues other tool for doing this. ignore these options')
            sys.exit(1)

    if not args.databases and not args.tables:
        filter_args = ' --all-databases'
    return filter_args


def config_login_path(args):
    if not args.reset:
        c_command = f'''
                    mysql_config_editor print -G {args.login_path} | grep password
                '''.replace('\n', ' ')
    else:
        c_command = f'''
                    mysql_config_editor remove -G {args.login_path}
                '''.replace('\n', ' ')
    if args.debug:
        logger.debug(c_command)
    resp = delegator.run(c_command)
    if resp.return_code != 0 or args.reset:
        s_command = f'''
                    mysql_config_editor set --login-path={args.login_path} --host={args.host} --password 
                    --user={args.user} --port={args.port}
                '''.replace('\n', ' ')
        if args.socket:
            s_command += f' --socket={args.socket}'
        if args.debug:
            logger.debug(s_command)
        set_msg = 'unset' if not args.reset else 'reset'
        logger.info(f'Login path {set_msg}, please enter the password for backup.')
        delegator.run(s_command)
    return


def delete_fail_backup_file(args):
    logger.info(f'Deleting backup file: {args.backup_file}')
    args.backup_file.unlink()
    if not args.backup_file.exists():
        logger.info('Delete success.')
    else:
        logger.info('Delete fail.')
    sys.exit(1)


def process_backup(args):
    logger.info(f'Process backup for mysql server {args.host}:{args.port} ...')
    logger.info(f'Log into logfile: {args.backup_log}')
    logger.info(f'Result save into: {args.backup_file}')

    connect_args = get_connect_args(args)
    command = f'{args.tool.name} {connect_args} {args.extra}'

    tmp_dir = args.backup_dir.absolute() / 'tmp'
    if args.tool.name in ['mydumper', 'xtrabackup']:
        tmp_dir.mkdir(parents=True, exist_ok=True)

    filter_args = add_filter_args(args)
    if args.tool.name == 'mysqldump':
        command += f''' 
            --master-data=2 --single-transaction --set-gtid-purged=AUTO --skip-tz-utc --complete-insert 
            --hex-blob --default-character-set utf8mb4 --routines --events --triggers --add-drop-table 
            --max-allowed-packet=256M --log-error={args.backup_log} {filter_args} | lz4 -z -9 -c 
            > {args.backup_file}
        '''.replace('\n', ' ')
    elif args.tool.name == 'mysqlpump':
        command += f'''
            --default-parallelism={args.threads} --single-transaction --set-gtid-purged=ON --skip-tz-utc 
            --add-drop-table --complete-insert --extended-insert=1000 --hex-blob --default-character-set utf8mb4
            --routines --events --triggers --log-error-file={args.backup_log} {filter_args} | lz4 -z -9 -c
            > {args.backup_file}
        '''.replace('\n', ' ')
    elif args.tool.name == 'mydumper':
        command += f'''
            --threads {args.threads} --trx-consistency-only --use-savepoints --triggers --events --routines 
            --skip-definer --compress --rows 100000 --skip-tz-utc --complete-insert --set-names utf8mb4 
            --disk-limits 1024:4096 --logfile {args.backup_log} -v 3 --stream -o {tmp_dir}
            > {args.backup_file}
        '''.replace('\n', ' ')
    else:

        history_dir = args.backup_dir.absolute() / 'history'
        history_dir.mkdir(parents=True, exist_ok=True)

        if args.incremental and (history_dir / 'xtrabackup_checkpoints').exists():
            get_lsn_command = f"""
                grep to_lsn {history_dir / 'xtrabackup_checkpoints'} | sed -r 's@to_lsn = @@g'
            """
            if args.debug:
                logger.debug(get_lsn_command)
            resp = delegator.run(get_lsn_command)
            args.extra += f''' --incremental-lsn={resp.out}'''

        if not args.login_path:
            command += ' --password=$MYSQL_PWD'
        command += f'''
            --backup --stream=xbstream --compress --compress-threads={args.threads} --parallel={args.threads}
            --target-dir={args.backup_dir} --tmpdir={tmp_dir} --extra-lsndir={history_dir} 
            2>>{args.backup_log} 1>{args.backup_file}
        '''.replace('\n', ' ')

    if args.debug:
        logger.debug(command)

    try:
        resp = delegator.run(command, env={'MYSQL_PWD': args.password})

        if resp.return_code == 0 and args.tool.name in ['mydumper', 'xtrabackup']:
            tmp_dir.rmdir()

        if resp.return_code != 0:
            logger.error(f'Could not process backup for mysql server: {args.host}:{args.port}.')
            if resp.err:
                logger.error(f'Got error: {resp.err}')
            else:
                logger.error(f'Please check logfile {args.backup_log} for get more error detail.')

            delete_fail_backup_file(args)
    except KeyboardInterrupt:
        delete_fail_backup_file(args)
        logger.exception('')

    logger.info('Backup success.')
    return


def main(args):
    add_extra_args(args)
    pre_backup(args)
    # check hang
    t = Thread(target=check_hung, args=(args,), daemon=True)
    t.start()
    # start backup
    process_backup(args)
    return


if __name__ == "__main__":
    command_line_args = parse_mysql_args_from_command_line(sys.argv[1:])
    main(command_line_args)
