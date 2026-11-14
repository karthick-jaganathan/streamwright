#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

import pytest

from streamwright.core.jobs.naming import NamingError, parse_template, plan_names

BATCH = "2026-10-17T23:59:30Z"


def test_seq_numbers_splits_in_value_order_so_a_replay_names_them_alike():
    template = parse_template("{country}_{seq}.csv", ["country"])
    splits = [(("US",), {"country": "US"}), (("CA",), {"country": "CA"}), (("GB",), {"country": "GB"})]
    names = plan_names(template, None, splits, BATCH, "b1")
    assert names == {("CA",): "CA_00001.csv", ("GB",): "GB_00002.csv", ("US",): "US_00003.csv"}
    assert plan_names(template, None, list(reversed(splits)), BATCH, "b1") == names


def test_dates_layouts_constants_and_collisions():
    layout = parse_template("dt={today:%Y-%m-%d}/{c}", ["c"], allow_slash=True, name="layout")
    file_name = parse_template("{c}-{host}-{today-1d}.csv", ["c"], constants=("host",))
    names = plan_names(file_name, layout, [(("a b",), {"c": "a b"})], BATCH, "b1", constants={"host": "h1"})
    assert names == {("a b",): "dt=2026-10-17/a_b/a_b-h1-20261016.csv"}
    with pytest.raises(NamingError, match="both render"):
        plan_names(parse_template("{c}.csv", ["c"]), None, [(("a b",), {"c": "a b"}), (("a_b",), {"c": "a_b"})],
                   BATCH, "b1")
    for bad in ("{c}/x.csv", "fixed.csv", "{nope}.csv", "{now:%T}"):
        with pytest.raises(NamingError):
            parse_template(bad, ["c"])
