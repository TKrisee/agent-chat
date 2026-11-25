import unittest

from agent_chat.core import CoordError
from agent_chat.turnover_queue import require_empty_queue, require_only_bootstrap


class TurnoverQueueTests(unittest.TestCase):
    def test_only_empty_queue_allows_add(self):
        require_empty_queue([])
        for queue in (None, [{'id': 'existing'}]):
            with self.subTest(queue=queue), self.assertRaises(CoordError):
                require_empty_queue(queue)

    def test_only_exact_single_item_allows_start(self):
        own = {'id': 'queue-1', 'clientUserMessageId': 'request-1'}
        require_only_bootstrap([own], 'request-1', 'queue-1')
        for queue in ([], [own, {'id': 'other'}],
                      [{'id': 'queue-1', 'clientUserMessageId': 'other'}],
                      [{'id': 'other', 'clientUserMessageId': 'request-1'}], [None]):
            with self.subTest(queue=queue), self.assertRaises(CoordError):
                require_only_bootstrap(queue, 'request-1', 'queue-1')


if __name__ == '__main__':
    unittest.main()
