'use strict';

angular.module(
    'stateSummary', [
        'ngResource'
    ])
    .directive('stateSummary', function factory() {
        return {
            restrict: 'E',
            scope: {
                clientId: '<',
                stateId: '<'
            },
            controller: [
                '$scope',
                '$resource',
                '$interval',
                function($scope, $resource, $interval) {
                    var self = this;

                    function resetScope() {
                        $scope.expiry = "";
                        $scope.fh = "";
                        $scope.revoked = undefined;
                        $scope.access = undefined;
                        $scope.deny = undefined;
                    }
                    resetScope();

                    $scope.accessRead = function() {
                        return ($scope.access & 1) != 0;
                    }
                    $scope.accessWrite = function() {
                        return ($scope.access & 2) != 0;
                    }
                    $scope.denyRead = function() {
                        return ($scope.deny & 1) != 0;
                    }
                    $scope.denyWrite = function() {
                        return ($scope.deny & 2) != 0;
                    }

                    self.State = $resource(
                        '/nfs4/client/:clientId/state/:stateId', {}, {}, {});
                    function getState() {
                        self.State.get(
                            {
                                clientId: $scope.clientId,
                                stateId: $scope.stateId
                            },
                            function(v) {
                                $scope.expiry = v.expiry;
                                $scope.fh = v.fh;
                                $scope.revoked = v.revoked;
                                $scope.access = v.access;
                                $scope.deny = v.deny;
                            });
                    }
                    $scope.$watch('stateId', function() {
                        if ($scope.stateId) {
                            getState();
                            /*
                            var timer = $interval(getClient, 1000);
                            $scope.$on('$destroy', function() {
                                $interval.cancel(timer);
                            });
                            */
                        }
                        else {
                            resetScope();
                        }
                    });
                }
            ],
            templateUrl: 'components/state-summary/state-summary.html'
        };
    });
