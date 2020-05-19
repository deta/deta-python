import os
import unittest
from deta import Deta
from timeit import timeit
from dotenv import load_dotenv


load_dotenv()


class TestBaseMethods(unittest.TestCase):
    def setUp(self):
        deta = Deta(os.getenv("DETA_BASE_PROJECT_KEY"))
        self.db = deta.Base("kjwhdeiuwehdfiuw")
        self.item1 = {"key": "existing1", "value": "test"}
        self.item2 = {"key": "existing2", "value": 7}
        self.item3 = {"key": "existing3", "value": 44}
        self.item4 = {"key": "existing4", "value": {"name": "patrick"}}
        self.db.put_many([self.item1, self.item2, self.item3, self.item4])

    def tearDown(self):
        all_items = next(self.db.fetch())
        for item in all_items:
            self.db.delete(item["key"])
        self.db.client.close()

    def test_put(self):
        resp = {"key": "one", "msg": "hello"}
        self.assertEqual(self.db.put({"msg": "hello"}, "one"), resp)
        self.assertEqual(self.db.put({"key": "one", "msg": "hello"}), resp)
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
        self.assertEqual(
            set(self.db.insert({"msg": "hello"}).keys()), set(["key", "msg"])
        )

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
        res4 = list(self.db.fetch(buffer=1, limit=4))
        res5 = next(self.db.fetch({"value.name": self.item4["value"]["name"]}))
        res6 = next(self.db.fetch({"value?gte": 7}))
        res7 = next(self.db.fetch([{"value?gt": 6}, {"value?lt": 50}]))
        self.assertTrue(len(res1) > 0)
        self.assertTrue(len(res2) == 0)
        self.assertTrue(len(res3) == 3)
        self.assertTrue(len(res4) == 4)
        self.assertTrue(len(res5) > 0)
        self.assertTrue(len(res6) == 2)
        self.assertTrue(len(res7) == 2)


if __name__ == "__main__":
    unittest.main()
