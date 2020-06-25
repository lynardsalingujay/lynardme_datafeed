Feature: Infer data for various assets

    Scenario Outline: Convert between different symbols for various assets
    Given "<symbol_in>", "<source_in>", "<security_type>" and "<source_out>"
    When I convert the symbol
    Then I get "<symbol_out>"

    Examples:
      | symbol_in | source_in | security_type | source_out |  symbol_out |
      | RTYU9         | Reyl      | index_future  | Bloomberg | RTYU19 Index |
      | RTYU18 Index  | Bloomberg | index_future  | Bloomberg | RTYU18 Index |
      | GBP/USD       | Reyl      | fx_spot | Bloomberg | GBPUSD Curncy |
      | GB0006778350  | Reyl      | fund    | Bloomberg | GB0006778350 ISIN |


    Scenario: Classify all the cash movements
      Given a file called "reyl_cash_movements.xlsx"
      When I parse the file
      Then All the CashMovement models have valid classifications

    Scenario Outline: Cash movements should have the correct classifications
      Given a cash movement description "<description>"
      When I classify the cash movement
      Then the classification should be "<classification>"

      Examples:
        | description | classification |
        | TRANSFER BETWEEN PERSONAL ACCOUNTS | transfer |
        | Subscr. 1097 JPM Japan -A- / ABR195026 | fund |
        | Your purchase USD/GBP 1.290153 | fx_spot |
        | NO. 5 1 9 8 9 0 | transfer |
        | Cash distrib. 641 JPM US Sel -A-, GBP 0.0046 | dividend |
        | Debit interest | interest |

    Scenario Outline: the enums should be exported for convenience and brevity

      Given I import the enums module
      Then "<name>" should be "<value>"

      Examples:
      | name | value |
      | reyl | Reyl  |
      | buy  | buy   |