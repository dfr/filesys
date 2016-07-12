'use strict';

angular.module(
    'clientList', [
        'ngResource',
        'ngMaterial',
        'clientSummary'
    ])
    .directive('clientList', function factory() {
        return {
            restrict: 'E',
            scope: {
            },
	    templateUrl: 'components/client-list/client-list.html',
            controller: [
                '$scope',
                '$interval',
                '$resource',
                function ctrl($scope, $interval, $resource) {
                    var self = this;

                    $scope.selected = null;
                    $scope.selectClient = function(id) {
                        $scope.selected = id;
                    }
                    $scope.clients = [];

                    self.Client = $resource('/nfs4/client', {}, {}, {});

                    function updateClients() {
                        self.Client.query(function(newList) {
                            newList.sort();
                            let oldList = $scope.clients;
                            let listChanged = false;
                            if (oldList.length != newList.length) {
                                listChanged = true;
                            }
                            else {
                                for (let i = 0; i < newList.length; i++) {
                                    if (oldList[i] != newList[i]) {
                                        listChanged = true;
                                    }
                                }
                            }
                            if (listChanged) {
                                if ($scope.selected) {
                                    let id = $scope.selected;
                                    $scope.selected = null;
                                    for (let i = 0; i < newList.length; i++) {
                                        if (newList[i] == id) {
                                            $scope.selected = newList[i];
                                            break;
                                        }
                                    }
                                }
                                if (!$scope.selected) {
                                    $scope.selected = newList[0];
                                }
                                $scope.clients = newList;
                            }
                        });
                    }

                    updateClients();
                    var timer = $interval(updateClients, 1000);
                    $scope.$on('$destroy', function() {
                        $interval.cancel(timer);
                    });
                }
            ]
        }
    });
