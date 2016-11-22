'use strict';

angular.module(
    'filesystemSummary', [
        'ngResource'
    ])
    .directive('filesystemSummary', function factory() {
        return {
            restrict: 'E',
            controller: [
                '$scope',
                '$resource',
                '$interval',
                function($scope, $resource, $interval) {
                    var self = this;

                    $scope.showFS = true;
                    $scope.hideDevs = false;
                    $scope.showDevs = true;
                    $scope.fsattr = {
                        totalSpace: null,
                        freeSpace: null,
                        availSpace: null,
                        totalFiles: null,
                        freeFiles: null,
                        availFiles: null,
                        repairQueueSize: null,
                    };
                    $scope.devices = [];

                    $scope.hideReplicas = false;
                    $scope.showReplicas = true;
                    $scope.replicas = [];

                    $scope.formatAddresses = function(addrs) {
                        let res = new Set();
                        for (let addr of addrs)
                            res.add(addr.host + ':' + addr.port);
                        return Array.from(res);
                    }

                    function stateColor(state) {
                        switch (state) {
                        case 'healthy':
                            return 'green';
                        case 'restoring':
                            return 'yellow';
                        case 'missing':
                            return 'orange';
                        case 'dead':
                        default:
                            return 'red';
                        }
                    }

		    $scope.humanizeNumber = function(val) {
		        let suffix = "KMGTPE";
		        let divisor = 1024.0;

		        if (val < divisor)
			    return String(val);

		        let v = Number(val) / divisor;
		        let i = 0;
		        while (v > divisor && i < 6) {
			    v /= divisor;
			    i++;
		        }
		        if (v < 10)
			    return v.toFixed(1) + suffix[i];
		        else
			    return v.toFixed(0) + suffix[i];
		    }

                    self.Filesystem = $resource('/fsattr', {}, {}, {});
                    function getFilesystem() {
                        function mergeArray(oldList, newList) {
                            let listChanged = false;
                            if (oldList.length != newList.length) {
                                listChanged = true;
                            }
                            else {
                                for (let i = 0; i < newList.length; i++) {
                                    if (oldList[i].id != newList[i].id) {
                                        listChanged = true;
                                    }
                                }
                            }
                            if (listChanged) {
                                return newList;
                            }
                            else {
                                for (let i = 0; i < newList.length; i++) {
                                    let oldEntry = oldList[i];
                                    let newEntry = newList[i];
                                    newEntry.color = stateColor(newEntry.state);
                                    for (let field in newEntry) {
                                        if (oldEntry[field] != newEntry[field])
                                            oldEntry[field] = newEntry[field];
                                    }
                                }
                            }
                            return oldList;
                        }

                        self.Filesystem.get(function(v) {
                            for (let k in $scope.fsattr) {
                                if ($scope.fsattr[k] != v.stats[k])
                                    $scope.fsattr[k] = v.stats[k];
                            }

                            if (v.devices.length == 0) {
                                $scope.hideDevs = true;
                            }
                            else {
                                $scope.devices = mergeArray(
                                    $scope.devices, v.devices);
                            }

                            if (v.replicas.length == 0) {
                                $scope.hideReplicas = true;
                            }
                            else {
                                $scope.replicas = mergeArray(
                                    $scope.replicas, v.replicas);
                            }
                        });
                    }
                    getFilesystem();
                    var timer = $interval(getFilesystem, 1000);
                    $scope.$on('$destroy', function() {
                        $interval.cancel(timer);
                    });
                }
            ],
            templateUrl: 'components/filesystem-summary/filesystem-summary.html'
        };
    });
