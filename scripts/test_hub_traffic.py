import unittest
from unittest.mock import Mock, patch

import httpx

import hub_traffic as ht


class HubTrafficTests(unittest.TestCase):
    def limiter(self):
        current = [0.0]
        pauses = []
        def sleep(seconds):
            pauses.append(seconds)
            current[0] += seconds
        return ht.HubRequestLimiter(clock=lambda: current[0], sleep=sleep), current, pauses

    def test_burst_is_spaced_across_api_and_resolver_requests(self):
        limiter, current, pauses = self.limiter()
        for i in range(600):
            path = '/api/datasets/test/data' if i % 2 else '/datasets/test/data/resolve/main/progress.json'
            limiter.request(httpx.Request('GET', 'https://' + limiter.host + path))
        self.assertEqual(current[0], 599)
        self.assertEqual(len(pauses), 599)
        limiter.request(httpx.Request('GET', 'https://cdn.example.net/large-parquet'))
        self.assertEqual(current[0], 599)

    def test_reported_low_allowance_stops_before_exhaustion(self):
        limiter, current, pauses = self.limiter()
        request = httpx.Request('GET', 'https://' + limiter.host + '/api/datasets/test/data')
        limiter.request(request)
        limiter.response(httpx.Response(200, request=request, headers={'RateLimit': '"api";r=100;t=90'}))
        limiter.request(request)
        self.assertEqual(current[0], 91)
        self.assertLessEqual(max(pauses), 30)
        limiter.response(httpx.Response(429, request=request, headers={'Retry-After': '7'}))
        limiter.request(request)
        self.assertEqual(current[0], 99)

    def test_sdk_factory_preserves_hooks_and_limits_nested_requests(self):
        limiter, current, _ = self.limiter()
        original_hook = Mock()
        client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200)),
                             event_hooks={'request': [original_hook]})
        with patch.object(ht, '_installed', False), patch.object(ht, 'set_client_factory') as install, \
                patch.object(ht, 'default_client_factory', return_value=client), \
                patch.object(ht, 'HubRequestLimiter', return_value=limiter):
            ht.install_hub_request_limiter()
            ht.install_hub_request_limiter()
            install.assert_called_once()
            with install.call_args.args[0]() as configured:
                configured.get('https://' + limiter.host + '/api/datasets/test/data')
                configured.post('https://' + limiter.host + '/api/datasets/test/data/commit/main')
            self.assertEqual(original_hook.call_count, 2)
            self.assertEqual(current[0], 1)


if __name__ == '__main__':
    unittest.main()
