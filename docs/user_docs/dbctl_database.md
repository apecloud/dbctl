---
title: dbctl database
---

specify database.

### Synopsis

```

```

```
dbctl database [flags]
```

### Examples

```

dbctl mongodb createuser --username root --password password
  
```

### Options

```
  -h, --help   help for database
```

### Options inherited from parent commands

```
      --add_dir_header                    If true, adds the file directory to the header of the log messages
      --alsologtostderr                   log to standard error as well as files (no effect when -logtostderr=true)
      --config-path string                dbctl default config directory for builtin type (default "/tools/config/dbctl/components/")
      --disable-dns-checker               disable dns checker, for test&dev
      --kubeconfig string                 Paths to a kubeconfig. Only required if out-of-cluster.
      --log_backtrace_at traceLocation    when logging hits line file:N, emit a stack trace (default :0)
      --log_dir string                    If non-empty, write log files in this directory (no effect when -logtostderr=true)
      --log_file string                   If non-empty, use this log file (no effect when -logtostderr=true)
      --log_file_max_size uint            Defines the maximum size a log file can grow to (no effect when -logtostderr=true). Unit is megabytes. If the value is 0, the maximum file size is unlimited. (default 1800)
      --logtostderr                       log to standard error instead of files (default true)
      --one_output                        If true, only write logs to their native severity level (vs also writing to each lower severity level; no effect when -logtostderr=true)
      --skip_headers                      If true, avoid header prefixes in the log messages
      --skip_log_headers                  If true, avoid headers when opening log files (no effect when -logtostderr=true)
      --stderrthreshold severity          logs at or above this threshold go to stderr when writing to files and stderr (no effect when -logtostderr=true or -alsologtostderr=true) (default 2)
      --tools-dir string                  The directory of tools binaries (default "/tools/")
  -v, --v Level                           number for the log level verbosity
      --vmodule moduleSpec                comma-separated list of pattern=N settings for file-filtered logging
      --zap-devel                         Development Mode defaults(encoder=consoleEncoder,logLevel=Debug,stackTraceLevel=Warn). Production Mode defaults(encoder=jsonEncoder,logLevel=Info,stackTraceLevel=Error) (default true)
      --zap-encoder encoder               Zap log encoding (one of 'json' or 'console')
      --zap-log-level level               Zap Level to configure the verbosity of logging. Can be one of 'debug', 'info', 'error', or any integer value > 0 which corresponds to custom debug levels of increasing verbosity
      --zap-stacktrace-level level        Zap Level at and above which stacktraces are captured (one of 'info', 'error', 'panic').
      --zap-time-encoding time-encoding   Zap time encoding (one of 'epoch', 'millis', 'nano', 'iso8601', 'rfc3339' or 'rfc3339nano'). Defaults to 'epoch'.
```

### SEE ALSO


* [dbctl database createuser](dbctl_database_createuser.md)	 - create user.
* [dbctl database deleteuser](dbctl_database_deleteuser.md)	 - delete user.
* [dbctl database describeuser](dbctl_database_describeuser.md)	 - describe user.
* [dbctl database getrole](dbctl_database_getrole.md)	 - get role of the replica.
* [dbctl database grant-role](dbctl_database_grant-role.md)	 - grant user role.
* [dbctl database joinmember](dbctl_database_joinmember.md)	 - execute a join member request.
* [dbctl database leavemember](dbctl_database_leavemember.md)	 - execute a leave member request.
* [dbctl database listsystemaccounts](dbctl_database_listsystemaccounts.md)	 - list system accounts.
* [dbctl database listusers](dbctl_database_listusers.md)	 - list normal users.
* [dbctl database revoke-role](dbctl_database_revoke-role.md)	 - revoke user role.
* [dbctl database service](dbctl_database_service.md)	 - Run dbctl as a daemon and provide api service.
* [dbctl database switchover](dbctl_database_switchover.md)	 - execute a switchover request.

#### Go Back to [dbctl Overview](dbctl.md) Homepage.

