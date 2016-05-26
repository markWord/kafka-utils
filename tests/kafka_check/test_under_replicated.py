# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import mock
import requests
from mock import MagicMock

from kafka_utils.kafka_check.commands.under_replicated import get_under_replicated
from kafka_utils.kafka_check.commands.under_replicated import get_under_replicated_from_broker


FAKE_PORT = 9090
FAKE_HOST0 = 'hostname0'
FAKE_HOST1 = 'hostname1'
FAKE_HOST2 = 'hostname2'
QUERY_URL = 'http://{host}:{port}/jolokia/read/kafka.cluster:type=Partition,name=UnderReplicated,topic=*,partition=*'
EMPTY_RESPONSE = '{"value": {}}'

RESPONSE = """{
    "value": {
        "kafka.cluster:name=UnderReplicated,partition=1,topic=topic2,type=Partition": {
            "Value": 0
        },
        "kafka.cluster:name=UnderReplicated,partition=3,topic=topic3,type=Partition": {
            "Value": 0
        },
        "kafka.cluster:name=UnderReplicated,partition=0,topic=topic0,type=Partition": {
            "Value": 2
        },
        "kafka.cluster:name=UnderReplicated,partition=0,topic=topic1,type=Partition": {
            "Value": 0
        },
        "kafka.cluster:name=UnderReplicated,partition=4,topic=topic4,type=Partition": {
            "Value": 1
        },
        "kafka.cluster:name=UnderReplicated,partition=0,topic=topic5,type=Partition": {
            "Value": 0
        }
    }
}"""


def _prepare_answer(content):
    mock = MagicMock(spec=requests.Response())
    mock.content = content
    return mock


@mock.patch(
    'kafka_utils.kafka_check.commands.under_replicated.requests.get',
    return_value=_prepare_answer(EMPTY_RESPONSE),
    autospec=True,
)
def test_get_under_replicated_from_broker_empty(mock_get):
    count = get_under_replicated_from_broker(FAKE_HOST0, FAKE_PORT)

    mock_get.assert_called_once_with(QUERY_URL.format(host=FAKE_HOST0, port=FAKE_PORT))
    assert mock_get.return_value.method_calls == [mock.call.raise_for_status()]
    assert count == 0


@mock.patch(
    'kafka_utils.kafka_check.commands.under_replicated.requests.get',
    return_value=_prepare_answer(RESPONSE),
    autospec=True,
)
def test_get_under_replicated_from_broker_regular(mock_get):
    count = get_under_replicated_from_broker(FAKE_HOST0, FAKE_PORT)

    mock_get.assert_called_once_with(QUERY_URL.format(host=FAKE_HOST0, port=FAKE_PORT))
    assert mock_get.return_value.method_calls == [mock.call.raise_for_status()]
    assert count == 3


def test_get_under_replicated_empty():
    assert get_under_replicated({}, FAKE_PORT) == 0


@mock.patch(
    'kafka_utils.kafka_check.commands.under_replicated.get_under_replicated_from_broker',
    return_value=2,
    autospec=True,
)
def test_get_under_replicated_regular(mock_call):
    brokers = {
        2: {'host': FAKE_HOST0},
        6: {'host': FAKE_HOST1},
        13: {'host': FAKE_HOST2},
    }

    count = get_under_replicated(brokers, FAKE_PORT)

    calls = [
        mock.call(FAKE_HOST0, FAKE_PORT),
        mock.call(FAKE_HOST1, FAKE_PORT),
        mock.call(FAKE_HOST2, FAKE_PORT),
    ]
    assert sorted(mock_call.mock_calls) == sorted(calls)
    assert count == 6  # number of under replicated partitions
