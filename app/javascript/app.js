var indexOf = function(needle) {
    if(typeof Array.prototype.indexOf === 'function') {
        indexOf = Array.prototype.indexOf;
    } else {
        indexOf = function(needle) {
            var i = -1, index = -1;

            for(i = 0; i < this.length; i++) {
                if(this[i] === needle) {
                    index = i;
                    break;
                }
            }

            return index;
        };
    }

    return indexOf.call(this, needle);
};

function sleep(milliseconds) {
  var start = new Date().getTime();
  for (var i = 0; i < 1e7; i++) {
    if ((new Date().getTime() - start) > milliseconds){
      break;
    }
  }
}

Array.prototype.remove = function() {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);
        }
    }
    return this;
};

(function() {
  var app = angular.module('TweetStream', ['ngAnimate']);

  app.value('loadingService', {
    loadingCount: 0,
    isLoading: function() { return loadingCount > 0; },
    requested: function() { loadingCount += 1; },
    responded: function() { loadingCount -= 1; }
  });

  app.factory('loadingInterceptor', ['$q', 'loadingService', function($q, loadingService) {
    return {
      request: function(config) {
        loadingService.requested();
        return config;
      },
      response: function(response) {
        loadingService.responded();
        return response;
      },
      responseError: function(rejection) {
        loadingService.responded();
        return $q.reject(rejection);
      },
    }
  }]);

  app.config(["$httpProvider", function ($httpProvider) {
    $httpProvider.interceptors.push('loadingInterceptor');
  }]);

  app.controller('TweetStreamController', ['$http', '$animate', '$scope', 'loadingService', function($http, $animate, $scope, loadingService) {
    loadingCount = 0;
    $scope.tweetstream.messagetitle = "";
    $scope.tweetstream.messagebody = "";
    $scope.tweetstream.jobs = [];
    this.start = function() {
      $http.post('/api/v1/start', {
        twitterconsumerkey: $scope.tweetstream.twitterconsumerkey,
        twitterconsumersecret: $scope.tweetstream.twitterconsumersecret,
        twitteraccesstoken: $scope.tweetstream.twitteraccesstoken,
        twitteraccesssecret: $scope.tweetstream.twitteraccesssecret,
        accesskey: $scope.tweetstream.accesskey,
        secretkey: $scope.tweetstream.secretkey,
        endpoint: $scope.tweetstream.endpoint,
        bucket: $scope.tweetstream.bucket,
        objectsize: $scope.tweetstream.objectsize,
        keywords: $scope.tweetstream.keywords,
      }).success(function(data) {
        $scope.tweetstream.status();
      }).error(function(data, status, headers, config) {
        $scope.tweetstream.messagetitle = "Error";
        $scope.tweetstream.messagebody = data;
        $('#message').modal('show');
      });
    }
    this.status = function() {
      $http.post('/api/v1/status', {
        twitterconsumerkey: $scope.tweetstream.twitterconsumerkey,
        twitterconsumersecret: $scope.tweetstream.twitterconsumersecret,
        twitteraccesstoken: $scope.tweetstream.twitteraccesstoken,
        twitteraccesssecret: $scope.tweetstream.twitteraccesssecret,
        accesskey: $scope.tweetstream.accesskey,
        secretkey: $scope.tweetstream.secretkey,
      }).success(function(data) {
        $scope.tweetstream.jobs = data;
      }).error(function(data, status, headers, config) {
        $scope.tweetstream.messagetitle = "Error";
        $scope.tweetstream.messagebody = data;
        $('#message').modal('show');
      });
    }
    this.stop = function(bucket, keywords) {
      $http.post('/api/v1/stop', {
        twitterconsumerkey: $scope.tweetstream.twitterconsumerkey,
        twitterconsumersecret: $scope.tweetstream.twitterconsumersecret,
        twitteraccesstoken: $scope.tweetstream.twitteraccesstoken,
        twitteraccesssecret: $scope.tweetstream.twitteraccesssecret,
        accesskey: $scope.tweetstream.accesskey,
        secretkey: $scope.tweetstream.secretkey,
        bucket: bucket,
        keywords: keywords,
      }).success(function(data) {
        $scope.tweetstream.messagetitle = "Job";
        $scope.tweetstream.messagebody = "Job will be stopped shortly. Click on Get Status to check if the job has already been stopped";
        $('#message').modal('show');
      }).error(function(data, status, headers, config) {
        $scope.tweetstream.messagetitle = "Error";
        $scope.tweetstream.messagebody = data;
        $('#message').modal('show');
      });
    }
  }]);

  app.directive("tweetstreamMessage", function() {
    return {
      restrict: 'E',
      templateUrl: "app/html/tweetstream-message.html"
    };
  });

  app.directive("tweetstreamDashboard", function() {
    return {
      restrict: 'E',
      templateUrl: "app/html/tweetstream-dashboard.html"
    };
  });
})();
