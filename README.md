# dbctl
dbctl is a service that provides command execution channels, originally found in KubeBlocks. It offers implementations of actions for various well-known database engines, such as apecloud-MySQL, MySQL, Redis, PostgreSQL, MongoDB, and others. For the database engines supported by dbctl, when integrating addons, you can declare dbctl as an engines plugin, enabling quick integration with KubeBlocks.

dbctl itself provides two running modes: daemon mode and temporary task mode. You can choose the appropriate mode based on your business scenario.

## Daemon Mode
In this mode, dbctl runs as a daemon process and provides API services. This can be treated as one form of implementing the engine plugin. KubeBlocks does not impose restrictions on the form of engines plugins; they can run as sidecars, container daemons, or other forms. Currently, dbctl uses the localhost address to communicate with the database processes by default. Therefore, in this mode, it is recommended to deploy dbctl using the sidecar method, with the deployment template as follows:
```
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: mysql
    image: apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/apecloud-mysql-server:8.0.30
    ...
  - name: dbctl
    image: apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/dbctl:0.2.0
    command:
    - dbctl
    - mysql
    - service
    env:
    - name: KB_POD_NAME
      valueFrom:
        fieldRef:
          apiVersion: v1
          fieldPath: metadata.name
    - name: KB_SERVICE_USER
      valueFrom:
        secretKeyRef:
          key: username
          name: cluster-mysql-account-root
    - name: KB_SERVICE_PASSWORD
      valueFrom:
        secretKeyRef:
          key: password
          name: cluster-mysql-account-root
```

When using dbctl in daemon mode, action definitions can be implemented by calling the dbctl API:
```
  lifecycleActions:
    roleProbe:
      exec:
        command:
          - /bin/bash
          - -c
          - curl -X GET -H 'Content-Type: application/json' 'http://127.0.0.1:5001/v1.0/getrole'
```

## Temporary Task
In this mode, dbctl completes the corresponding task and then exits immediately. It is suitable for use as a tool and can be called directly in actions. For a list of supported actions and their usage, please refer to the documentation in the [docs](docs/user_docs/dbctl.md).

```
  lifecycleActions:
    roleProbe:
      exec:
        image: apecloud-registry.cn-zhangjiakou.cr.aliyuncs.com/apecloud/dbctl:0.2.0
        command:
          - dbctl
          - mysql
          - getrole
```
