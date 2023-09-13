import datetime
import io
import os
import random
import string
import unittest
from pathlib import Path

from deta import Deta
from deta.drive import UPLOAD_CHUNK_SIZE
from deta.base import FetchResponse

try:
    from dotenv import load_dotenv

    load_dotenv()
except:
    pass


"""
class TestSendEmail(unittest.TestCase):
    def setUp(self):
        self.deta = Deta()

    def test_function(self):
        self.assertIsNone(
            send_email("mustafa@deta.sh", "Hello from test", "this is a test!")
        )

    def test_method(self):
        self.assertIsNone(
            self.deta.send_email(
                "mustafa@deta.sh", "Hello from test", "this is a test!"
            )
        )"""


class TestDriveMethods(unittest.TestCase):
    def setUp(self) -> None:
        key = os.getenv("DETA_SDK_TEST_PROJECT_KEY")
        name = os.getenv("DETA_SDK_TEST_DRIVE_NAME")
        host = os.getenv("DETA_SDK_TEST_DRIVE_HOST")
        self.assertIsNotNone(key)
        self.assertIsNotNone(name)
        deta = Deta(key)
        self.drive = deta.Drive(name, host=host)
        return super().setUp()

    def tearDown(self) -> None:
        all_items = self.drive.list()
        for item in all_items["names"]:
            self.drive.delete(item)

    def test_put_string(self):
        test_cases = [
            {"name": "test_file_1.txt", "content": "this is a string."},
            {"name": "name with spaces.txt", "content": "lorem ipsum"},
            {
                "name": "test_file_1.txt",
                "content": "same file name should be overwritten",
            },
        ]
        for tc in test_cases:
            name = self.drive.put(tc["name"], tc["content"])
            self.assertEqual(name, tc["name"])
            self.assertEqual(self.drive.get(tc["name"]).read().decode(), tc["content"])

    def test_put_bytes(self):
        test_cases = [
            {"name": "byte_file.txt", "content": b"bytes content"},
            {"name": "another bytes file.txt", "content": b"another bytes content"},
        ]
        for tc in test_cases:
            name = self.drive.put(tc["name"], tc["content"])
            self.assertEqual(name, tc["name"])
            self.assertEqual(self.drive.get(tc["name"]).read(), tc["content"])

    def test_put_stream(self):
        test_cases = [
            {
                "name": "string_stream.txt",
                "raw": b"string stream",
                "content": io.StringIO("string stream"),
            },
            {
                "name": "binary_stream.txt",
                "raw": b"binary stream",
                "content": io.BytesIO(b"binary stream"),
            },
        ]
        for tc in test_cases:
            name = self.drive.put(tc["name"], tc["content"])
            self.assertEqual(name, tc["name"])
            self.assertEqual(self.drive.get(tc["name"]).read(), tc["raw"])
            self.assertEqual(tc["content"].closed, True)

    def test_large_file(self):
        name = "large_binary_file"
        large_binary_file = os.urandom(UPLOAD_CHUNK_SIZE * 2 + 1000)
        self.assertEqual(self.drive.put("large_binary_file", large_binary_file), name)

        body = self.drive.get(name)
        binary_stream = io.BytesIO(large_binary_file)
        for chunk in body.iter_chunks(UPLOAD_CHUNK_SIZE):
            self.assertEqual(chunk, binary_stream.read(UPLOAD_CHUNK_SIZE))

    def test_delete(self):
        test_cases = [
            {"name": "to_del_1.txt", "content": "hello"},
            {"name": "to del name with spaces.txt", "content": "hola"},
        ]
        for tc in test_cases:
            self.drive.put(tc["name"], tc["content"])
            self.assertEqual(self.drive.delete(tc["name"]), tc["name"])
            self.assertIsNone(self.drive.get(tc["name"]))

    def test_delete_many(self):
        test_cases = [
            {"name": "to_del_1.txt", "content": "hello"},
            {"name": "to del name with spaces.txt", "content": "hola"},
        ]
        for tc in test_cases:
            self.drive.put(tc["name"], tc["content"])

        names = [tc["name"] for tc in test_cases]
        deleted = self.drive.delete_many(names)["deleted"]
        self.assertIn(names[0], deleted)
        self.assertIn(names[1], deleted)

        for n in names:
            self.assertIsNone(self.drive.get(n))

    def test_list(self):
        test_cases = [
            {"name": "a", "content": "a"},
            {"name": "b", "content": "b"},
            {"name": "c/d", "content": "c and d"},
        ]
        for tc in test_cases:
            self.drive.put(tc["name"], tc["content"])

        self.assertEqual(self.drive.list()["names"], ["a", "b", "c/d"])
        self.assertEqual(self.drive.list(limit=1)["names"], ["a"])
        self.assertEqual(self.drive.list(limit=2)["paging"]["last"], "b")
        self.assertEqual(self.drive.list(prefix="c/")["names"], ["c/d"])

    def test_read_close(self):
        test_cases = [
            {
                "name": "string_stream.txt",
                "raw": b"string stream",
                "content": io.StringIO("string stream"),
            },
        ]
        for tc in test_cases:
            self.drive.put(tc["name"], tc["content"])
            body = self.drive.get(tc["name"])
            body.close()
            self.assertEqual(body.closed, True)

    def test_read_lines(self):
        test_cases = [
            {
                "name": "read_lines_test.txt",
                "content": "first line\nSecond line\nLast Line\n",
            },
            {
                "name": "read_lines_test_2.txt",
                "content": "has no new lines, just a normal string",
            },
            {
                "name": "read_lines_test_3.txt",
                "content": "different new line\ranother line\r",
            },
        ]
        for tc in test_cases:
            test_stream = io.StringIO(tc["content"])
            self.drive.put(tc["name"], tc["content"])
            body = self.drive.get(tc["name"])
            for line in body.iter_lines():
                self.assertEqual(test_stream.readline(), line.decode())


class TestBaseMethods(unittest.TestCase):
    def setUp(self):
        key = os.getenv("DETA_SDK_TEST_PROJECT_KEY")
        name = os.getenv("DETA_SDK_TEST_BASE_NAME")
        self.assertIsNotNone(key)
        self.assertIsNotNone(name)
        deta = Deta(key)
        self.db = deta.Base(str(name))
        self.ttl_attribute = os.getenv("DETA_SDK_TEST_TTL_ATTRIBUTE") or "__expires"
        self.item1 = {"key": "existing1", "value": "test"}
        self.item2 = {"key": "existing2", "value": 7}
        self.item3 = {"key": "existing3", "value": 44}
        self.item4 = {"key": "existing4", "value": {"name": "patrick"}}
        self.item5 = {"key": "%@#//#!#)#$_", "value": 0, "list": ["a"]}
        self.db.put_many([self.item1, self.item2, self.item3, self.item4, self.item5])

    def tearDown(self):
        items = self.db.fetch().items
        for i in items:
            self.db.delete(i["key"])
        self.db.client.close()

    def test_put(self):
        item = {"msg": "hello"}
        resp = {"key": "one", "msg": "hello"}
        example_path = Path(__file__).parent / ".."
        self.assertEqual(self.db.put(item, "one"), resp)
        self.assertEqual(self.db.put(item, "one"), resp)
        self.assertEqual({"msg": "hello"}, item)
        self.assertEqual(
            self.db.put({"example_path": example_path}, "example_key"),
            {"example_path": example_path.resolve().as_posix(), "key": "example_key"}
        )
        self.assertEqual(set(self.db.put("Hello").keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(1).keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(True).keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(False).keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(3.14159265359).keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(example_path).keys()), set(["key", "value"]))

    @unittest.expectedFailure
    def test_put_fail(self):
        self.db.put({"msg": "hello"}, 1)
        self.db.put({"msg": "hello", "key": True})

    def test_put_many(self):
        self.assertEqual(len(self.db.put_many([1, 2, 3])["processed"]["items"]), 3)
        ok = self.db.put_many([{"msg": "hello"}, {"msg2": "hi"}])["processed"]["items"]
        self.assertEqual(len(ok), 2)

    @unittest.expectedFailure
    def test_put_many_fail(self):
        self.db.put_many([{"name": "joe", "key": "ok"}, {"name": "mo", "key": 7}])

    @unittest.expectedFailure
    def test_put_many_fail_limit(self):
        self.db.put_many([i for i in range(26)])

    def test_insert(self):
        item = {"msg": "hello"}
        self.assertEqual(set(self.db.insert(item).keys()), set(["key", "msg"]))
        self.assertEqual({"msg": "hello"}, item)

    @unittest.expectedFailure
    def test_insert_fail(self):
        self.db.insert(self.item1)

    def test_get(self):
        self.assertEqual(self.db.get(self.item1["key"]), self.item1)
        self.assertIsNone(self.db.get("key_does_not_exist"))

    def test_delete(self):
        self.assertIsNone(self.db.delete(self.item1["key"]))
        self.assertIsNone(self.db.delete("key_does_not_exist"))

    def test_fetch(self):
        res1 = self.db.fetch({"value?gte": 7})
        expectedItem = FetchResponse(
            2,
            None,
            [
                {"key": "existing2", "value": 7},
                {"key": "existing3", "value": 44},
            ],
        )
        self.assertEqual(res1, expectedItem)

        res2 = self.db.fetch({"value?gte": 7}, limit=1)
        expectedItem = FetchResponse(
            1,
            "existing2",
            [
                {"key": "existing2", "value": 7},
            ],
        )
        self.assertEqual(res2, expectedItem)

        res3 = self.db.fetch([{"value?gt": 6}, {"value?lt": 50}], limit=2)
        expectedItem = FetchResponse(
            2,
            "existing2",
            [
                {"key": "%@#//#!#)#$_", "list": ["a"], "value": 0},
                {"key": "existing2", "value": 7},
            ],
        )
        self.assertEqual(res3, expectedItem)

        res4 = self.db.fetch(
            [{"value?gt": 6}, {"value?lt": 50}], limit=2, last="existing2"
        )
        expectedItem = FetchResponse(
            1,
            None,
            [{"key": "existing3", "value": 44}],
        )
        self.assertEqual(res4, expectedItem)

        res5 = self.db.fetch({"value": "test"})
        expectedItem = FetchResponse(
            1,
            None,
            [{"key": "existing1", "value": "test"}],
        )
        self.assertEqual(res5, expectedItem)

        res6 = self.db.fetch({"valuexyz": "test_none_existing_value"})
        expectedItem = FetchResponse(
            0,
            None,
            [],
        )
        self.assertEqual(res6, expectedItem)

        res7 = self.db.fetch({"value.name": self.item4["value"]["name"]})
        expectedItem = FetchResponse(
            1,
            None,
            [{"key": "existing4", "value": {"name": "patrick"}}],
        )
        self.assertEqual(res7, expectedItem)

        res8 = self.db.fetch({"value?gte": 7}, desc=True)
        expectedItem = FetchResponse(
            2,
            None,
            [
                {"key": "existing3", "value": 44},
                {"key": "existing2", "value": 7},
            ],
        )
        self.assertEqual(res8, expectedItem)

    def test_update(self):
        self.assertIsNone(self.db.update({"value.name": "spongebob"}, "existing4"))
        expectedItem = {"key": "existing4", "value": {"name": "spongebob"}}
        self.assertEqual(self.db.get("existing4"), expectedItem)

        self.assertIsNone(
            self.db.update(
                {"value.name": self.db.util.trim(), "value.age": 32}, "existing4"
            )
        )
        expectedItem = {"key": "existing4", "value": {"age": 32}}
        self.assertEqual(self.db.get("existing4"), expectedItem)

        self.assertIsNone(
            self.db.update(
                {
                    "list": self.db.util.append(["b", "c"]),
                    "value": self.db.util.increment(),
                },
                "%@#//#!#)#$_",
            )
        )

        self.assertIsNone(
            self.db.update(
                {"list": self.db.util.prepend("x"), "value": self.db.util.increment(2)},
                "%@#//#!#)#$_",
            )
        )
        expectedItem = {"key": "%@#//#!#)#$_", "list": ["x", "a", "b", "c"], "value": 3}
        self.assertEqual(self.db.get("%@#//#!#)#$_"), expectedItem)

        # key does not exist
        self.assertRaises(Exception, self.db.update, {"value": "test"}, "doesNotExist")
        # deleting a key
        self.assertRaises(
            Exception,
            self.db.update,
            {"value": "test", "key": self.db.util.trim()},
            "existing4",
        )
        # updating a key
        self.assertRaises(Exception, self.db.update, {"key": "test"}, "existing4")
        # upper hierarchy does not exist
        self.assertRaises(Exception, self.db.update, {"profile.age": 32}, "existing4")
        # no attributes specified
        self.assertRaises(Exception, self.db.update, {}, "existing4")

        # appending to a key
        self.assertRaises(
            Exception,
            self.db.update,
            {"key": self.db.util.append("test")},
            "%@#//#!#)#$_",
        )

    def get_expire_at(self, expire_at):
        return int(expire_at.replace(microsecond=0).timestamp())

    def get_expire_in(self, expire_in):
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds=expire_in)
        return self.get_expire_at(expire_at)

    def test_ttl(self):
        expire_in = 300
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds=300)
        delta = 2  # allow time delta of 2 seconds
        test_cases = [
            {
                "item": self.item1,
                "expire_in": expire_in,
                "expected_ttl_value": self.get_expire_in(expire_in),
                "delta": delta,
            },
            {
                "item": self.item1,
                "expire_at": expire_at,
                "expected_ttl_value": self.get_expire_at(expire_at),
                "delta": delta,
            },
            {
                "item": self.item2,
                "expected_ttl_value": None,
                "delta": delta,
            },
            {
                "item": self.item1,
                "expire_in": expire_in,
                "expire_at": expire_at,
                "delta": delta,
                "expected_ttl_value": None,
                "error": ValueError,
            },
            {
                "item": self.item1,
                "expire_in": "randomtest",
                "expected_ttl_value": None,
                "delta": delta,
                "error": TypeError,
            },
            {
                "item": self.item1,
                "expire_at": "not a datetime, int or float",
                "expected_ttl_value": None,
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
                self.db.put(item, expire_in=cexp_in, expire_at=cexp_at)
                got = self.db.get(item.get("key"))
                self.assertAlmostEqual(
                    expected, got.get(self.ttl_attribute), delta=cdelta
                )

                # insert
                # need to udpate key as insert does not allow pre existing key
                item["key"] = "".join(random.choices(string.ascii_lowercase, k=6))
                self.db.insert(item, expire_in=cexp_in, expire_at=cexp_at)
                got = self.db.get(item.get("key"))
                self.assertAlmostEqual(
                    expected, got.get(self.ttl_attribute), delta=cdelta
                )

                # put many
                self.db.put_many([item], expire_in=cexp_in, expire_at=cexp_at)
                got = self.db.get(item.get("key"))
                self.assertAlmostEqual(
                    expected, got.get(self.ttl_attribute), delta=cdelta
                )

                # update
                # only if one of expire_in or expire_at
                if cexp_in or cexp_at:
                    self.db.update(
                        None, item.get("key"), expire_in=cexp_in, expire_at=cexp_at
                    )
                    got = self.db.get(item.get("key"))
                    self.assertAlmostEqual(
                        expected, got.get(self.ttl_attribute), delta=cdelta
                    )
            else:
                self.assertRaises(
                    error, self.db.put, item, expire_in=cexp_in, expire_at=cexp_at
                )
                self.assertRaises(
                    error, self.db.insert, item, expire_in=cexp_in, expire_at=cexp_at
                )
                self.assertRaises(
                    error,
                    self.db.put_many,
                    [item],
                    expire_in=cexp_in,
                    expire_at=cexp_at,
                )
                self.assertRaises(
                    error,
                    self.db.update,
                    None,
                    item.get("key"),
                    expire_in=cexp_in,
                    expire_at=cexp_at,
                )


if __name__ == "__main__":
    unittest.main()
