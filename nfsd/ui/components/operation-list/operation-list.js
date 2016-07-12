'use strict';

angular.module(
    'operationList', [
        'ngResource',
        'ngMaterial'
    ])
    .directive('operationList', function factory() {
        return {
            restrict: 'E',
            scope: {
            },
	    templateUrl: 'components/operation-list/operation-list.html',
            controller: [
                '$scope',
                '$interval',
                '$resource',
                function($scope, $interval, $resource)
                {
                    var self = this;

                    $scope.showV4 = true;
                    $scope.showV3 = true;

                    $scope.ops4 = [];
                    $scope.ops3 = [];

                    self.Data4 = $resource('/nfs4', {}, {}, {});
                    self.Data3 = $resource('/nfs3', {}, {}, {});
                    function updateList() {
                        function mergeArray(oldList, v) {
                            let newList = [];
                            for (let key in v.operations) {
                                newList.push({
                                    name: key,
                                    count: v.operations[key]
                                });
                            }
                            newList.sort(function(x, y) {
                                if (x.name < y.name)
                                    return -1;
                                if (x.name > y.name)
                                    return 1;
                                return 0;
                            });

                            let listChanged = false;
                            if (oldList.length != newList.length) {
                                listChanged = true;
                            }
                            else {
                                for (let i = 0; i < newList.length; i++) {
                                    if (oldList[i].name != newList[i].name) {
                                        listChanged = true;
                                    }
                                }
                            }
                            if (listChanged) {
                                return newList;
                            }
                            else {
                                for (let i = 0; i < newList.length; i++) {
                                    if (oldList[i].count != newList[i].count)
                                        oldList[i] = newList[i];
                                }
                            }
                            return oldList;
                        }
                        self.Data4.get(function(v) {
                           $scope.ops4 = mergeArray($scope.ops4, v);
                        });
                        self.Data3.get(function(v) {
                           $scope.ops3 = mergeArray($scope.ops3, v);
                        });
                    }
                    updateList();
                    var timer = $interval(updateList, 1000);
                    $scope.$on('$destroy', function() {
                        $interval.cancel(timer);
                    });
                }
            ]
        };
    });
