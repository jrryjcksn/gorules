package main

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/jrryjcksn/go-sqlite3"
	. "github.com/jrryjcksn/gorules/pkg/rules"
	//xsxs	appsv1 "k8s.io/api/apps/v1"
)

// var dep = `{
//     "apiVersion": "apps/v1",
//     "kind": "Deployment",
//     "metadata": {
//         "name": "coredns",
//         "namespace": "kube-system"
//     },
//     "spec": {
//         "replicas": 1
//     }
// }`

var dep = `{
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "annotations": {
            "deployment.kubernetes.io/revision": "1"
        },
        "creationTimestamp": "2021-11-30T18:41:24Z",
        "generation": 1,
        "labels": {
            "k8s-app": "kube-dns"
        },
        "name": "coredns",
        "namespace": "kube-system",
        "resourceVersion": "624",
        "uid": "99658090-79c3-4192-8c05-4d5efc9a164f"
    },
    "spec": {
        "progressDeadlineSeconds": 600,
        "replicas": 1,
        "revisionHistoryLimit": 10,
        "selector": {
            "matchLabels": {
                "k8s-app": "kube-dns"
            }
        },
        "strategy": {
            "rollingUpdate": {
                "maxSurge": "25%",
                "maxUnavailable": 1
            },
            "type": "RollingUpdate"
        },
        "template": {
            "metadata": {
                "creationTimestamp": null,
                "labels": {
                    "k8s-app": "kube-dns"
                }
            },
            "spec": {
                "containers": [
                    {
                        "args": [
                            "-conf",
                            "/etc/coredns/Corefile"
                        ],
                        "image": "k8s.gcr.io/coredns/coredns:v1.8.0",
                        "imagePullPolicy": "IfNotPresent",
                        "livenessProbe": {
                            "failureThreshold": 5,
                            "httpGet": {
                                "path": "/health",
                                "port": 8080,
                                "scheme": "HTTP"
                            },
                            "initialDelaySeconds": 60,
                            "periodSeconds": 10,
                            "successThreshold": 1,
                            "timeoutSeconds": 5
                        },
                        "name": "coredns",
                        "ports": [
                            {
                                "containerPort": 53,
                                "name": "dns",
                                "protocol": "UDP"
                            },
                            {
                                "containerPort": 53,
                                "name": "dns-tcp",
                                "protocol": "TCP"
                            },
                            {
                                "containerPort": 9153,
                                "name": "metrics",
                                "protocol": "TCP"
                            }
                        ],
                        "readinessProbe": {
                            "failureThreshold": 3,
                            "httpGet": {
                                "path": "/ready",
                                "port": 8181,
                                "scheme": "HTTP"
                            },
                            "periodSeconds": 10,
                            "successThreshold": 1,
                            "timeoutSeconds": 1
                        },
                        "resources": {
                            "limits": {
                                "memory": "170Mi"
                            },
                            "requests": {
                                "cpu": "100m",
                                "memory": "70Mi"
                            }
                        },
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "capabilities": {
                                "add": [
                                    "NET_BIND_SERVICE"
                                ],
                                "drop": [
                                    "all"
                                ]
                            },
                            "readOnlyRootFilesystem": true
                        },
                        "terminationMessagePath": "/dev/termination-log",
                        "terminationMessagePolicy": "File",
                        "volumeMounts": [
                            {
                                "mountPath": "/etc/coredns",
                                "name": "config-volume",
                                "readOnly": true
                            }
                        ]
                    }
                ],
                "dnsPolicy": "Default",
                "nodeSelector": {
                    "kubernetes.io/os": "linux"
                },
                "priorityClassName": "system-cluster-critical",
                "restartPolicy": "Always",
                "schedulerName": "default-scheduler",
                "securityContext": {},
                "serviceAccount": "coredns",
                "serviceAccountName": "coredns",
                "terminationGracePeriodSeconds": 30,
                "tolerations": [
                    {
                        "key": "CriticalAddonsOnly",
                        "operator": "Exists"
                    },
                    {
                        "effect": "NoSchedule",
                        "key": "node-role.kubernetes.io/master"
                    },
                    {
                        "effect": "NoSchedule",
                        "key": "node-role.kubernetes.io/control-plane"
                    }
                ],
                "volumes": [
                    {
                        "configMap": {
                            "defaultMode": 420,
                            "items": [
                                {
                                    "key": "Corefile",
                                    "path": "Corefile"
                                }
                            ],
                            "name": "coredns"
                        },
                        "name": "config-volume"
                    }
                ]
            }
        }
    },
    "status": {
        "availableReplicas": 2,
        "conditions": [
            {
                "lastTransitionTime": "2021-11-30T18:41:59Z",
                "lastUpdateTime": "2021-11-30T18:41:59Z",
                "message": "Deployment has minimum availability.",
                "reason": "MinimumReplicasAvailable",
                "status": "True",
                "type": "Available"
            },
            {
                "lastTransitionTime": "2021-11-30T18:41:39Z",
                "lastUpdateTime": "2021-11-30T18:42:00Z",
                "message": "ReplicaSet \"coredns-558bd4d5db\" has successfully progressed.",
                "reason": "NewReplicaSetAvailable",
                "status": "True",
                "type": "Progressing"
            }
        ],
        "observedGeneration": 1,
        "readyReplicas": 2,
        "replicas": 2,
        "updatedReplicas": 2
    }
}`

func TestRule(t *testing.T) {
	//func main() {
	RuleSet(
		"test",
		Rule(Name("rule1"),
			Conditions(
				Match("Deployment", "foo", Namespace("kube-system"), LT(Field("spec", "replicas"), Number(1000)))),
			Actions(
				func(c *RuleContext) error {
					field, err := c.GetIntField("foo", 0, "spec", "replicas")
					if err != nil {
						fmt.Printf("%v\n", err)
						return err
					}

					fmt.Printf("replicas: %d\n", field)

					// sfield, err := c.GetStringField("foo", "", "metadata", "name")
					// if err != nil {
					//  return err
					// }

					// fmt.Printf("name: %s\n", sfield)

					foo, err := c.Delete("foo")
					if err != nil {
						return err
					}

					c.Set(foo, Field("spec", "replicas"), field+1)
					c.Add(foo)

					return nil
				})))

	//	e, err := NewEngine("file:/tmp/db", "test")
	e, err := NewEngine("", "test")

	if err != nil {
		fmt.Printf("got error creating engine: %v\n", err)
	}

	e.AddResourceStringList([]string{dep})

	start := time.Now()

	if err := e.Run(); err != nil {
		fmt.Printf("got error running engine: %v\n", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Time: %s\n", elapsed)
}

//  database, err := sql.Open("sqlite3", "file:storage?mode=memory")
//  if err != nil {
//      fmt.Printf("ERROR: %v\n", err)
//  }

//  s, _ := database.Prepare("create table resources (id integer primary key, kind text, namespace text, name text, data json)")
//  s.Exec()

//  s, _ = database.Prepare(`insert into resources (kind, namespace, name, data) values ('Ball', 'test', 'foo', '{"kind": "Ball", "namespace": "test", "name": "foo", "color": "red", "size": 10}')`)
//  s.Exec()

//  rows, err := database.Query("SELECT id, kind, namespace, name, json_array(json_extract(data, '$.color')) from resources")
//  if err != nil {
//      fmt.Printf("ERROR: %v\n", err)
//  }

//  var id int
//  var kind, namespace, name, data string

//  for rows.Next() {
//      rows.Scan(&id, &kind, &namespace, &name, &data)
//      fmt.Printf("%d:%s:%s:%s:%s\n", id, kind, namespace, name, data)
//  }
// }
