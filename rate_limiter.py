import api_service

RATE_LIMIT_CONFIG = {
    "serviceLimits": [{"service": "OrderService", "globalLimits": {"GET": {"limit": 10, "granularity": "second"},
                                                                   "POST": {"limit": 50, "granularity": "minute"}},
                       "apiLimits": [{"methods": {"GET": {"limit": 15, "granularity": "second"},
                                                  "POST": {"limit": 5, "granularity": "minute"}},
                                      "api": "CreateOrder"}, {"methods": {"GET": {"limit": 10, "granularity": "second"},
                                                                          "POST": {"limit": 10,
                                                                                   "granularity": "second"}},
                                                              "api": "GetOrderById"}]}, {"service": "DeliveryService",
                                                                                         "globalLimits": {
                                                                                             "GET": {"limit": 3,
                                                                                                     "granularity": "second"},
                                                                                             "POST": {"limit": 20,
                                                                                                      "granularity": "minute"}},
                                                                                         "apiLimits": []}]
}


import time
current_milli_time = lambda: int(round(time.time() * 1000))

class RateLimiter(api_service.ApiService):

    def __init__(self):
        super(RateLimiter, self).__init__()
        self.rate_limit_config = self.__configParser()
        self.GLOBAL_RATE_LIMITS = {}
        self.API_RATE_LIMITS = {}

    def make_request(self, api_endpoint=""):

        call_time = current_milli_time()
        service_endpoint, local_api_endpoint, method = api_endpoint.split(':')

        if not self.__is_call_allowed(service_endpoint, local_api_endpoint, method, call_time):
            print("Rate Limited, call not allowed")
            return

        print("Call allowed !!")
        super(RateLimiter, self).make_request(api_endpoint)

    def __is_call_allowed(self, service_endpoint, api_endpoint, method, call_time):

        if self.__satisfyGlobalLimits(service_endpoint, method, call_time) and self.__satisfyApiLimits(service_endpoint, api_endpoint, method, call_time):

            # We will update hits only when both the conditions are satisfied
            self.__updateGlobalHits(service_endpoint, method, call_time)
            self.__updateApiHits(service_endpoint, api_endpoint, method, call_time)
            return True
        else: return False

    def __satisfyGlobalLimits(self, service_endpoint, method, call_time):

        global_hits = self.__getGlobalHits(service_endpoint, method)
        limit = self.rate_limit_config[service_endpoint]['global_limit'][method]['limit']
        granularity = self.__getGranularity(self.rate_limit_config[service_endpoint]['global_limit'][method]['granularity'])

        idx = 0
        for hit in global_hits:
            if hit <= (call_time - granularity):
                idx = idx+1
            else: break

        global_hits = global_hits[idx:]

        if len(global_hits) > limit:
            return False

        return True

    def __satisfyApiLimits(self, service_endpoint, api_endpoint, method, call_time):

        api_hits = self.__getApiHits(service_endpoint, api_endpoint, method)
        limit = self.rate_limit_config[service_endpoint]['api_limits'][api_endpoint][method]['limit']
        granularity = self.__getGranularity(self.rate_limit_config[service_endpoint]['api_limits'][api_endpoint][method]['granularity'])

        idx = 0
        for hit in api_hits:
            if hit <= (call_time - granularity):
                idx = idx + 1
            else:
                break

        api_hits = api_hits[idx:]

        if len(api_hits) > limit:
            return False

        return True

    def __configParser(self):
        service_limits = {}

        for obj in RATE_LIMIT_CONFIG["serviceLimits"]:
            service_limits[obj['service']] = {'global_limit': self.__globalLimitParser(obj['globalLimits']), 'api_limits': self.__apiLimitParser(obj['apiLimits'])}

        return service_limits

    def __globalLimitParser(self, globalLimits):
        return globalLimits

    def __apiLimitParser(self, apiLimits):
        api_limits = {}
        for obj in apiLimits:
            api_limits[obj['api']] = obj['methods']

        return api_limits

    def __getGlobalHits(self, service_endpoint, method):

        try:
            return self.GLOBAL_RATE_LIMITS[service_endpoint][method]
        except:
            if not service_endpoint in self.GLOBAL_RATE_LIMITS:
                self.GLOBAL_RATE_LIMITS[service_endpoint] = {}
            if not method in self.GLOBAL_RATE_LIMITS[service_endpoint]:
                self.GLOBAL_RATE_LIMITS[service_endpoint][method] = []
            return []

    def __getApiHits(self, service_endpoint, api_endpoint, method):
        try:
            return self.API_RATE_LIMITS[service_endpoint][api_endpoint][method]
        except:
            if not service_endpoint in self.API_RATE_LIMITS:
                self.API_RATE_LIMITS[service_endpoint] = {}
            if not api_endpoint in self.API_RATE_LIMITS[service_endpoint]:
                self.API_RATE_LIMITS[service_endpoint][api_endpoint] = {}
            if not method in self.API_RATE_LIMITS[service_endpoint][api_endpoint]:
                self.API_RATE_LIMITS[service_endpoint][api_endpoint][method] = []
            return []

    def __updateGlobalHits(self, service_endpoint, method, call_time):
        hits = self.__getGlobalHits(service_endpoint, method)
        hits.append(call_time)
        pass

    def __updateApiHits(self, service_endpoint, api_endpoint, method, call_time):
        hits = self.__getApiHits(service_endpoint, api_endpoint, method)
        hits.append(call_time)
        pass

    def __getGranularity(self, granularity):
        if granularity == 'second':
            return 1000
        elif granularity == 'minute':
            return 60*1000
