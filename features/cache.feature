# Created by alex at 12/09/2019
Feature: We should be able to easily cache stuff
  We want a decorator that allows easy caching of functions

  Scenario: The decorator function uses Django's underlying cache correctly
    Given a function that is decorated with "cache_result"
    When the function is called
    Then the result is in the cache at first but then expires

