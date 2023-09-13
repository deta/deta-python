from deta.base import FetchResponse
import datetime
import os
import pytest
import random
import string
from deta import Deta

try:
    from dotenv import load_dotenv

    load_dotenv()
except:
    pass

pytestmark = pytest.mark.asyncio


PROJECT_KEY = os.getenv("DETA_SDK_TEST_PROJECT_KEY")
BASE_NAME = os.getenv("DETA_SDK_TEST_BASE_NAME")
BASE_TEST_TTL_ATTRIBUTE = os.getenv("DETA_SDK_TEST_TTL_ATTRIBUTE")


@pytest.fixture()
async def db():
    assert PROJECT_KEY
    assert BASE_NAME
    assert BASE_TEST_TTL_ATTRIBUTE

    deta = Deta(PROJECT_KEY)
    db = deta.AsyncBase(BASE_NAME)

    yield db

    result = await db.fetch()
    for i in result.items:
        await db.delete(i["key"])
    await db.close()


@pytest.fixture()
async def items(db):
    items = [
        {"key": "existing1", "value": "test"},
        {"key": "existing2", "value": 7},
        {"key": "existing3", "value": 44},
        {"key": "existing4", "value": {"name": "patrick"}},
        {"key": "%@#//#!#)#$_", "value": 0, "list": ["a"]},
    ]
    await db.put_many(items)
    yield items


async def test_put(db):
    item = {"msg": "hello"}
    expected_resp = {"key": "one", "msg": "hello"}
    resp = await db.put(item, "one")
    assert resp == expected_resp
    assert {"msg": "hello"} == item

    for input in ["Hello", 1, True, False, 3.14159265359]:

        resp = await db.put(input)
        assert set(resp.keys()) == set(["key", "value"])


async def test_put_fail(db):
    with pytest.raises(Exception):
        await db.put({"msg": "hello"}, 1)
        await db.put({"msg": "hello", "key": True})


async def test_put_many(db):
    resp = await db.put_many([1, 2, 3])
    assert len(resp["processed"]["items"]) == 3

    resp = await db.put_many([{"msg": "hello"}, {"msg2": "hi"}])
    ok = resp["processed"]["items"]
    assert len(ok) == 2


async def test_put_many_fail(db):
    with pytest.raises(Exception):
        await db.put_many([{"name": "joe", "key": "ok"}, {"name": "mo", "key": 7}])


async def test_put_many_fail_limit(db):
    with pytest.raises(Exception):
        await db.put_many([i for i in range(26)])


async def test_insert(db):
    item = {"msg": "hello"}
    resp = await db.insert(item)
    assert set(resp.keys()) == set(["key", "msg"])


async def test_insert_fail(db, items):
    with pytest.raises(Exception):
        await db.insert(items[0])


async def test_get(db, items):
    resp = await db.get(items[0]["key"])
    assert resp == items[0]

    resp = await db.get("key_does_not_exist")
    assert resp is None


async def test_delete(db, items):
    resp = await db.delete(items[0]["key"])
    assert resp is None

    resp = await db.delete("key_does_not_exist")
    assert resp is None


async def test_fetch(db, items):
    res1 = await db.fetch({"value?gte": 7})
    expectedItem = FetchResponse(
        2,
        None,
        [
            {"key": "existing2", "value": 7},
            {"key": "existing3", "value": 44},
        ],
    )
    assert res1 == expectedItem

    res2 = await db.fetch({"value?gte": 7}, limit=1)
    expectedItem = FetchResponse(
        1,
        "existing2",
        [
            {"key": "existing2", "value": 7},
        ],
    )
    assert res2 == expectedItem

    res3 = await db.fetch([{"value?gt": 6}, {"value?lt": 50}], limit=2)
    expectedItem = FetchResponse(
        2,
        "existing2",
        [
            {"key": "%@#//#!#)#$_", "list": ["a"], "value": 0},
            {"key": "existing2", "value": 7},
        ],
    )
    assert res3 == expectedItem

    res4 = await db.fetch(
        [{"value?gt": 6}, {"value?lt": 50}], limit=2, last="existing2"
    )
    expectedItem = FetchResponse(
        1,
        None,
        [{"key": "existing3", "value": 44}],
    )
    assert res4 == expectedItem

    res5 = await db.fetch({"value": "test"})
    expectedItem = FetchResponse(
        1,
        None,
        [{"key": "existing1", "value": "test"}],
    )
    assert res5 == expectedItem

    res6 = await db.fetch({"valuexyz": "test_none_existing_value"})
    expectedItem = FetchResponse(
        0,
        None,
        [],
    )
    assert res6 == expectedItem

    res7 = await db.fetch({"value.name": items[3]["value"]["name"]})
    expectedItem = FetchResponse(
        1,
        None,
        [{"key": "existing4", "value": {"name": "patrick"}}],
    )
    assert res7 == expectedItem

    res8 = await db.fetch({"value?gte": 7}, desc=True)
    expectedItem = FetchResponse(
        2,
        None,
        [
            {"key": "existing3", "value": 44},
            {"key": "existing2", "value": 7},
        ],
    )
    assert res8 == expectedItem


async def test_update(db, items):
    resp = await db.update({"value.name": "spongebob"}, "existing4")
    assert resp is None

    resp = await db.get("existing4")
    expectedItem = {"key": "existing4", "value": {"name": "spongebob"}}
    assert resp == expectedItem

    resp = await db.update({"value.name": db.util.trim(), "value.age": 32}, "existing4")

    assert resp is None
    expectedItem = {"key": "existing4", "value": {"age": 32}}
    resp = await db.get("existing4")

    assert resp == expectedItem

    resp = await db.update(
        {
            "list": db.util.append(["b", "c"]),
            "value": db.util.increment(),
        },
        "%@#//#!#)#$_",
    )
    assert resp is None

    resp = await db.update(
        {"list": db.util.prepend("x"), "value": db.util.increment(2)},
        "%@#//#!#)#$_",
    )
    assert resp is None
    expectedItem = {"key": "%@#//#!#)#$_", "list": ["x", "a", "b", "c"], "value": 3}
    resp = await db.get("%@#//#!#)#$_")
    assert resp == expectedItem

    # key does not exist
    with pytest.raises(Exception):
        await db.update({"value": "test"}, "doesNotExist")

    # deleting a key
    with pytest.raises(Exception):
        await db.update({"value": "test", "key": db.util.trim()}, "existing4")

    # updating a key
    with pytest.raises(Exception):
        await db.update({"key": "test"}, "existing4")

    # upper hierarchy does not exist
    with pytest.raises(Exception):
        await db.update({"profile.age": 32}, "existing4")

    # no attributes specified
    with pytest.raises(Exception):
        await db.update({}, "existing4")

    # appending to a key
    with pytest.raises(Exception):
        await db.update(
            {"key": db.util.append("test")},
            "%@#//#!#)#$_",
        )


def get_expire_at(expire_at):
    return int(expire_at.replace(microsecond=0).timestamp())


def get_expire_in(expire_in):
    expire_at = datetime.datetime.now() + datetime.timedelta(seconds=expire_in)
    return get_expire_at(expire_at)


async def test_ttl(db, items):
    item1 = items[0]
    expire_in = 300
    expire_at = datetime.datetime.now() + datetime.timedelta(seconds=300)
    delta = 2  # allow time delta of 2 seconds
    test_cases = [
        {
            "item": item1,
            "expire_in": expire_in,
            "expected_ttl_value": get_expire_in(expire_in),
            "delta": delta,
        },
        {
            "item": item1,
            "expire_at": expire_at,
            "expected_ttl_value": get_expire_at(expire_at),
            "delta": delta,
        },
        {
            "item": item1,
            "expire_in": expire_in,
            "expire_at": expire_at,
            "delta": delta,
            "error": ValueError,
        },
        {
            "item": item1,
            "expire_in": "randomtest",
            "delta": delta,
            "error": TypeError,
        },
        {
            "item": item1,
            "expire_at": "not a datetime, int or float",
            "error": TypeError,
            "delta": delta,
        },
    ]

    for case in test_cases:
        item = case.get("item")
        cexp_in = case.get("expire_in")
        cexp_at = case.get("expire_at")
        expected = case.get("expected_ttl_value")
        error = case.get("error")
        cdelta = case.get("delta")

        if not case.get("error"):
            # put
            await db.put(item, expire_in=cexp_in, expire_at=cexp_at)
            got = await db.get(item.get("key"))
            assert abs(expected - got.get(BASE_TEST_TTL_ATTRIBUTE)) <= cdelta

            # insert
            # need to udpate key as insert does not allow pre existing key
            item["key"] = "".join(random.choices(string.ascii_lowercase, k=6))
            await db.insert(item, expire_in=cexp_in, expire_at=cexp_at)
            got = await db.get(item.get("key"))
            assert abs(expected - got.get(BASE_TEST_TTL_ATTRIBUTE)) <= cdelta

            # put many
            await db.put_many([item], expire_in=cexp_in, expire_at=cexp_at)
            got = await db.get(item.get("key"))
            assert abs(expected - got.get(BASE_TEST_TTL_ATTRIBUTE)) <= cdelta

            # update
            # only if one of expire_in or expire_at
            if cexp_in or cexp_at:
                await db.update(
                    None, item.get("key"), expire_in=cexp_in, expire_at=cexp_at
                )
                got = await db.get(item.get("key"))
                assert abs(expected - got.get(BASE_TEST_TTL_ATTRIBUTE)) <= cdelta
        else:
            with pytest.raises(error):
                await db.put(item, expire_in=cexp_in, expire_at=cexp_at)
            with pytest.raises(error):
                await db.put_many([item], expire_in=cexp_in, expire_at=cexp_at)
            with pytest.raises(error):
                await db.insert(item, expire_in=cexp_in, expire_at=cexp_at)
            with pytest.raises(error):
                await db.update(
                    None, item.get("key"), expire_in=cexp_in, expire_at=cexp_at
                )
