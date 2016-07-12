'use strict';

angular.module(
    'stateList', [
        'stateSummary'
    ])
    .directive('stateList', function factory() {
        return {
            restrict: 'E',
            scope: {
                clientId: '<',
                list: '<'
            },
	    templateUrl: 'components/state-list/state-list.html',
            controller: [
                '$scope',
                function($scope) {
                    $scope.selected = null;
                    $scope.selectState = function(id) {
                        $scope.selected = id;
                    }
                    $scope.$watch('list', function() {
                        if ($scope.selected) {
                            if ($scope.list.indexOf($scope.selected) == -1)
                                $scope.selected = null;
                        }
                        else {
                            if ($scope.list.length > 0)
                                $scope.selected = $scope.list[0];
                        }
                    });
                }
            ]
        };
    });
