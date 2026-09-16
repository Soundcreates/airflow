# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest import mock

import pytest
from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.utils.delete_from import (
    FailToDeleteError,
    delete_from_dict,
    delete_from_yaml,
)


@mock.patch(
    "airflow.providers.cncf.kubernetes.utils.delete_from.client.AppsV1Api",
    autospec=True,
)
def test_delete_from_dict_deletes_a_namespaced_resource(api_class):
    api_client = mock.Mock()
    api = mock.Mock(spec=["delete_namespaced_deployment"])
    body = {"propagationPolicy": "Foreground"}
    data = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "worker", "namespace": "jobs"},
    }

    api_class.return_value = api
    delete_from_dict(api_client, data, body, namespace="default", grace_period_seconds=10)

    api_class.assert_called_once_with(api_client)
    api.delete_namespaced_deployment.assert_called_once_with(
        name="worker",
        namespace="jobs",
        body=body,
        grace_period_seconds=10,
    )


@mock.patch(
    "airflow.providers.cncf.kubernetes.utils.delete_from.client.CoreV1Api",
    autospec=True,
)
def test_delete_from_dict_deletes_a_cluster_scoped_resource(api_class):
    api_client = mock.Mock()
    api = mock.Mock(spec=["delete_namespace"])
    data = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "jobs"},
    }

    api_class.return_value = api
    delete_from_dict(api_client, data, body=None, namespace="default")

    api.delete_namespace.assert_called_once_with(
        name="jobs",
        body=mock.ANY,
    )


@mock.patch(
    "airflow.providers.cncf.kubernetes.utils.delete_from.client.CoreV1Api",
    autospec=True,
)
def test_delete_from_dict_expands_list_items_and_collects_api_errors(api_class):
    api_client = mock.Mock()
    api_exception = ApiException(status=404, reason="Not Found")
    api_exception.body = "missing"
    api = mock.Mock(spec=["delete_namespaced_pod"])
    api.delete_namespaced_pod.side_effect = [api_exception, mock.Mock()]
    data = {
        "apiVersion": "v1",
        "kind": "PodList",
        "items": [
            {"apiVersion": "v1", "kind": "OldKind", "metadata": {"name": "first"}},
            {"apiVersion": "v1", "kind": "OldKind", "metadata": {"name": "second"}},
        ],
    }

    api_class.return_value = api
    with pytest.raises(FailToDeleteError) as error:
        delete_from_dict(api_client, data, body=None, namespace="default")

    assert error.value.api_exceptions == [api_exception]
    assert str(error.value) == "Error from server (Not Found):missing\n"
    assert all(item["kind"] == "Pod" for item in data["items"])
    api.delete_namespaced_pod.assert_has_calls(
        [
            mock.call(name="first", namespace="default", body=mock.ANY),
            mock.call(name="second", namespace="default", body=mock.ANY),
        ]
    )


@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict")
def test_delete_from_yaml_skips_empty_documents(delete_from_dict_mock):
    api_client = mock.Mock()
    objects = [None, {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "worker"}}]

    delete_from_yaml(k8s_client=api_client, yaml_objects=objects, namespace="jobs")

    delete_from_dict_mock.assert_called_once_with(
        k8s_client=api_client,
        data=objects[1],
        body=None,
        namespace="jobs",
        verbose=False,
    )
