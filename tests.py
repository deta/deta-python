from deta.drive import UPLOAD_CHUNK_SIZE
import os
import io
import unittest
from deta import Deta

try:
    from dotenv import load_dotenv

    load_dotenv()
except:
    pass


"""class TestSendEmail(unittest.TestCase):
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
                "content": "different new line\ranother line\r"
            }

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
        self.item1 = {"key": "existing1", "value": "test"}
        self.item2 = {"key": "existing2", "value": 7}
        self.item3 = {"key": "existing3", "value": 44}
        self.item4 = {"key": "existing4", "value": {"name": "patrick"}}
        self.item5 = {"key": "%@#//#!#)#$_", "value": 0, "list": ["a"]}
        self.db.put_many([self.item1, self.item2, self.item3, self.item4, self.item5])

    def tearDown(self):
        for items in self.db.fetch():
            for i in items:
                self.db.delete(i["key"])
        self.db.client.close()

    def test_put(self):
        item = {"msg": "hello"}
        resp = {"key": "one", "msg": "hello"}
        self.assertEqual(self.db.put(item, "one"), resp)
        self.assertEqual(self.db.put(item, "one"), resp)
        self.assertEqual({"msg": "hello"}, item)
        self.assertEqual(set(self.db.put("Hello").keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(1).keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(True).keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(False).keys()), set(["key", "value"]))
        self.assertEqual(set(self.db.put(3.14159265359).keys()), set(["key", "value"]))

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
        res1 = next(self.db.fetch({"value": "test"}))
        res2 = next(self.db.fetch({"valuexyz": "test_none_existing_value"}))
        res3 = next(self.db.fetch(buffer=3))
        res4 = list(self.db.fetch(buffer=1, pages=4))
        res5 = next(self.db.fetch({"value.name": self.item4["value"]["name"]}))
        res6 = next(self.db.fetch({"value?gte": 7}))
        res7 = next(self.db.fetch([{"value?gt": 6}, {"value?lt": 50}]))
        self.assertTrue(len(res1) > 0)
        self.assertTrue(len(res2) == 0)
        self.assertTrue(len(res3) == 3)
        self.assertTrue(len(res4) == 4)
        self.assertTrue(len(res5) > 0)
        self.assertTrue(len(res6) == 2)
        self.assertTrue(len(res7) == 3)

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


if __name__ == "__main__":
    unittest.main()
