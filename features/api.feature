# Created by alex at 02/10/2019
Feature: The back-end API should provide everything that the front-end needs
  # Enter feature description here

  Scenario: Obtain JWT tokens
    Given a user
    When the front-end requests a new token
    Then the back-end provides a valid JWT token

  Scenario: Authorize GET using JWT tokens
    Given a user
    And a valid JWT token
    When a protected url is requested
    Then the authorized response is valid
    And the unauthorized response is not valid

  Scenario: Authorize POST using JWT tokens
    Given a user
    And a valid JWT token
    When a file is posted to a protected url
    Then the authorized response is valid
    And the unauthorized response is not valid

  Scenario: futures price setting
    Given a future transaction is in the database with no price set
    And a user
    And a valid JWT token
    When the price is posted
    Then the price should be updated

  Scenario Outline: cash reconciliation summary
    Given some transactions and cash movements in the database
    And a valid JWT token
    When various requests with different query "<parameters>" are made
    Then there should not be any errors

    Examples:
    | parameters |
    | ?date=2018-01-01 |
    | ?errors_only=1&group_by=classification,value_date&date=2019-03-12 |
    | ? |
    | ?group_by=currency |

    Scenario Outline: be able to POST json model data and save it to the database
      Given some "<json>" representations of models
      And a user
      And a valid JWT token
      When the json is posted
      Then the model should be updated in the database

      Examples:
      | json |
      | {"model": "Price", "meta": "", "data": [{"value": 1587.20, "source": "Reyl", "time": "2019-09-01", "as_of": "2019-09-01", "symbol": "TPH9 Index", "resolution": "1d", "asset_type": "index_future"}]} |

