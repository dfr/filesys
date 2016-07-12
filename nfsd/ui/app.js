'use strict';

angular.module(
    'nfsdApp',
    [
        'ngMaterial',
        'ngRoute',
        'ngResource',
        'clientList',
        'operationList',
        'filesystemSummary'
    ])
    .config([
        '$locationProvider',
        '$routeProvider',
        '$mdIconProvider',
        function config($locationProvider, $routeProvider, $mdIconProvider) {
            $locationProvider.hashPrefix('!');
            $routeProvider
                .when(
                    '/clients', {
                        template: '<client-list></client-list>'
                    })
                .when(
                    '/stats', {
                        template: (
                            '<filesystem-summary></filesystem-summary>' +
                            '<br/>' +
                            '<operation-list></operation-list>')
                    })
                .otherwise('/stats');
            $mdIconProvider
                .icon('menu-open', 'assets/menu-open.svg')
                .icon('menu-closed', 'assets/menu-closed.svg');
        }])
    .controller(
        'NfsdController',
        ['$scope',
         '$location',
         '$resource',
         function AppCtrl($scope, $location, $resource) {
             if ($location.path().length > 0)
                 $scope.currentNavItem = $location.path().slice(1);
             else
                 $scope.currentNavItem = "stats";
             $scope.version = $resource('/version', {}, {}, {}).get();
         }]);
