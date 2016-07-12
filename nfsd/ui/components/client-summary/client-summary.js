'use strict';

angular.module(
    'clientSummary', [
        'ngResource',
        'stateList'
    ])
    .directive('clientSummary', function factory() {
        return {
            restrict: 'E',
            scope: {
                id: '<'
            },
            controller: [
                '$scope',
                '$resource',
                '$interval',
                '$http',
                '$mdDialog',
                function($scope, $resource, $interval, $http, $mdDialog) {
                    var self = this;

                    $scope.confirmed = true;
                    $scope.sessions = [];
                    $scope.opens = [];
                    $scope.delegations = [];
                    $scope.layouts = [];
                    $scope.sessionInfo = {};
                    $scope.channels = '';

                    $scope.revoke = function(ev) {
                        let confirm = $mdDialog.confirm()
                            .title('Revoke client state?')
                            .textContent('This forcibly revokes opens, delegations and layouts, allowing other clients to use the underlying files')
                            .ariaLabel('Revoke state')
                            .targetEvent(ev)
                            .ok('Ok')
                            .cancel('Cancel');
                        $mdDialog.show(confirm).then(
                            function() {
                                let url = [
                                    "/nfs4/client",
                                    $scope.id,
                                    "revoke"
                                ].join('/');
                                $http.post(url, true);
                            },
                            function() {
                            });
                    }

                    function mergeArray(name, newValue) {
                        let oldValue = $scope[name];
                        newValue.sort();
                        let changed = false;
                        if (oldValue.length != newValue.length) {
                            changed = true;
                        }
                        else {
                            for (let i = 0; i < newValue.length; i++) {
                                if (oldValue[i] != newValue[i]) {
                                    changed = true;
                                }
                            }
                        }
                        if (changed)
                            $scope[name] = newValue;
                    }

                    self.Client = $resource(
                        '/nfs4/client/:clientId', {}, {}, {});
                    function getClient() {
                        self.Client.get(
                            {clientId: $scope.id},
                            function(v) {
                                $scope.confirmed = v.confirmed;
                                mergeArray('sessions', v.sessions);
                                mergeArray('opens', v.opens);
                                mergeArray('delegations', v.delegations);
                                mergeArray('layouts', v.layouts);
                            });
                    }
                    $scope.$watch('id', function() {
                        if ($scope.id) {
                            getClient();
                            var timer = $interval(getClient, 1000);
                            $scope.$on('$destroy', function() {
                                $interval.cancel(timer);
                            });
                        }
                    });
                    self.Session = $resource(
                        '/nfs4/session/:sessionId', {}, {}, {});
                    $scope.$watch('sessions', function() {
                        $scope.sessionInfo = {};
                        for (let id of $scope.sessions) {
                            $scope.sessionInfo[id] = self.Session.get(
                                {sessionId: id},
                                function() {
                                    let addrs = [];
                                    for (let id in $scope.sessionInfo) {
                                        let chans =
                                            $scope.sessionInfo[id].channels;
                                        for (let chan of chans)
                                            addrs.push(chan);
                                    }
                                    $scope.channels = addrs.join(' ');
                                });
                        }
                    });
                }
            ],
            templateUrl: 'components/client-summary/client-summary.html'
        };
    });
