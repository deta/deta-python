import os
import unittest
from deta import Deta, send_email
from timeit import timeit
from dotenv import load_dotenv


load_dotenv()


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
        all_items = next(self.db.fetch())
        for item in all_items:
            self.db.delete(item["key"])
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
        self.assertEqual(
            set(self.db.insert(item).keys()), set(["key", "msg"])
        )
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
