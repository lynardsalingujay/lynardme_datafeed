
  Feature: logging is properly configured

    Scenario: basic logging happens
      Given a dev log configuration
      When I log a warning
      Then It is printed to stdout

    Scenario Outline: logging occurs at the correct levels
      Given a cloud logging configuration with "<name>", "<use_google_logging>", "<is_production>"
      When I log at "<level>"
      Then It is printed to stdout "<truth>"
      Examples:
      | name | use_google_logging | is_production | level | truth |
      | root | False | False | INFO | True |
      | root | False | False | DEBUG | True |
      | root | False | TRUE | WARNING | True |
