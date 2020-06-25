# Created by alex at 27/08/2019

Feature: Parse files
  Users should be able to upload transaction files (from brokers etc.) and the application should parse and save them to the database.

  Scenario Outline: bulk_create does not create same model twice
    Given a "<model_class>"
    And no "<model_class>" items in the database
    When I create a "<model_class>" twice using bulk_create
    Then there is only one "<model_class>" in the db
    Examples:
    | model_class |
    | Price |
    | CashMovement |
    | Transaction  |
    | FundClassification |
    | Position           |

  Scenario Outline: Parse files into models
    Given a file called "<file_name>"
    When I parse the file
    Then the result should be a valid "<model>"
    Examples:
      | file_name                 | model        |
      | static_universe_test.csv  | Universe     |
      | 5m_price_test.csv         | Price        |
      | daily_price_test.csv      | Price        |
      | reyl_fx_rates.xlsx        | Price        |
      | reyl_positions_1.xlsx     | Position     |
      | exante_trades.xls         | Transaction  |
      | exante_transactions.xls   | Transaction  |
      | reyl_transactions.xlsx    | Transaction  |
      | reyl_cash_movements.xlsx  | CashMovement |
      | exante_positions.csv      | Position     |
      | reyl_legacy/reyl_cash_movements_legacy_gbp.xlsx  | CashMovement |
      | reyl_legacy/reyl_cash_movements_legacy_usd.xlsx  | CashMovement |
      | reyl_legacy/reyl_cash_movements_legacy_jpy.xlsx  | CashMovement |
      | reyl_legacy/reyl_transactions_legacy.xlsx        | Transaction  |

  Scenario Outline: Do not duplicate transactions
    Given a file called "<file_name>"
    When I parse the file
    Then There should be only one "<transaction_type>" transaction for "<symbol>" with "<value_date>", "<quantity>", "<currency>" and "<tax>"
    Examples:
      | file_name | transaction_type | symbol | value_date | quantity | currency | tax |
      | exante_transactions.xls | fee | USD | 2019-08-20 | -5.0 | USD | 0                |
      | exante_transactions.xls | interest | USD | 2019-08-26 | -18.07 | USD | 0                |
      | exante_transactions.xls | transfer | GBP | 2019-08-14 | 39981.33 | GBP | 0                |
      | exante_trades.xls       | buy      | GBPJPY Curncy | 2019-08-28 | 40000 | JPY | 0      |
      | exante_trades.xls       | sell     | NKU19 Index | 2019-08-26 | 2 | JPY | 0      |
      | reyl_transactions.xlsx | sell | GBPUSD Curncy | 2019-08-27 | -6600000 | USD | 0 |
      | reyl_transactions.xlsx | sell | RTYU19 Index   | 2019-08-19 | -41 | USD | 0 |
      | reyl_transactions_2.xlsx | fee | GBP | 2019-06-30 | -238.03 | GBP | 0 |
      | reyl_transactions_2.xlsx | fee | GBP | 2019-06-17 | 2000 | GBP | 0 |
      | reyl_legacy/reyl_transactions_legacy.xlsx | fee | GBP | 2019-03-29 | -1835.63 |GBP | 0 |
      | reyl_legacy/reyl_transactions_legacy.xlsx | buy | GB00B1XN4Y45 ISIN | 2019-03-29 | 1079914 | GBP | -2996.76 |
      | reyl_transactions_3.xlsx | buy | NHZ19 Index | 2019-09-24 | 93 | JPY | 0 |
      | reyl_legacy/reyl_transactions_legacy.xlsx | transfer | GBP | 2019-01-17 | 3000000 | GBP | 0 |


  Scenario Outline: Calculate the FX fees correctly

    Given a file called "<file_name>"
    When I parse the file
    Then The direct fees should be "<direct_fee>" bps and the indirect fees should be "<indirect_fee>" bps of the gross transaction value

    Examples:
    | file_name | direct_fee | indirect_fee |
    | reyl_transactions.xlsx | 1 | 1 |
    | reyl_legacy/reyl_transactions_legacy.xlsx | 4 | 1 |


  Scenario Outline:

    Given a file called "<file_name>"
    When I parse the file
    Then there are no starting or trailing spaces in the following string fields: "<fields>"
    
    Examples:
      | file_name | fields |
      | reyl_transactions.xlsx | currency,asset_name,symbol |
      | reyl_cash_movements.xlsx | currency |


  Scenario: futures should not have price based fields set

    Given a file called "reyl_transactions.xlsx"
    When I parse the file
    Then transactions of type "index_future" have null fields "price,gross_transaction_value,net_transaction_value"


  Scenario Outline: parsing the same file twice should not save more objects to the database

    Given a file called "<file_name>"
    And no "<model_class>" items in the database
    When I parse the file and save the "<model_class>" models twice
    Then there should be the same number of models after each save

    Examples:
    | file_name | model_class |
    | reyl_transactions.xlsx | Transaction |
    | reyl_cash_movements.xlsx | CashMovement |
    | exante_trades.xls | Transaction |
    | exante_transactions.xls | Transaction |
    | 5m_price_test.csv | Price |
    | daily_price_test.csv | Price |
    | static_universe_test.csv | Universe |

  Scenario Outline: parsing price data

    Given a file called "<file_name>"
    When I parse the contents of the file
    Then There should be "<quantity>" price records

    Examples:
    | file_name | quantity |
    | 5m_price_test.csv | 116 |
    | daily_price_test.csv | 103 |

  Scenario Outline: parsing monthly 5m bar price

    Given a file called "<file_name>"
    When I parse the contents of the file
    Then The timezone of the as_of and time column should be "<timezone>"

    Examples:
    | file_name | timezone |
    | 5m_price_test.csv | UTC |

  Scenario Outline: parsing static universe data

    Given a file called "<file_name>"
    When I parse the contents of the file
    Then There should be "<quantity>" None values

    Examples:
    | file_name | quantity |
    | static_universe_test.csv | 84 |

  Scenario Outline: parsing position files

    Given a file called "<file_name>"
    When I parse the file
    Then There should be only one "<asset_type>" position for "<symbol>" with "<as_of_date>", "<quantity>" and "<currency>"

    Examples:
    | file_name | asset_type | symbol | as_of_date | quantity | currency |
    | exante_positions.csv | cash | GBP | 2019-08-23 | 49970.53 | GBP |
    | exante_positions.csv | cash | USD | 2019-08-23 | -154.97 | USD |
    | exante_positions.csv | fx | GBPUSD Curncy | 2019-08-23 | 140000 | USD |
    | exante_positions.csv | index_future | ESU19 Index | 2019-08-23 | -1 | USD |

  Scenario Outline: parse future trade confirmations

    Given a file called "<file_name>"
    When I parse the contents of the file
    Then the "<price>", "<gross_transaction_value>" and "<net_transaction_value>" should be correct

    Examples:
      | file_name | price | gross_transaction_value | net_transaction_value |
      | reyl_legacy/TPH9_settlement.json | 1587.20 | 79360000 | 79387245 |
      | reyl_legacy/26.03.19_FutureOpen_ESM9.pdf | 2819.864583 | -3383837.4996 | -3383541.4996 |
      | reyl_legacy/12.02.19_FutureOpen_S&PE-Mini500.pdf | 2725.618421 | -5178674.9999 | -5178322.9999 |
      | reyl_future_close.pdf | 1610.50 | 16105000  | 16131167 |
      | reyl_future_open.pdf | 2917    | -23336000  | -23335040 |


  Scenario: match futures confirmations to transactions

    Given I have parsed the legacy Reyl transactions
    When I parse the legacy futures confirms
    Then the futures prices should be set


  Scenario Outline: json files can be used to correct poor data in the parsed files

    Given a file called "<error_file_name>"
    When I parse the file and the the "<correcting_json_file_name>"
    Then the "<field>" for "<unique>" should have "<value>"

    Examples:
      | error_file_name        | correcting_json_file_name | field            | unique              | value                     |
      | reyl_transactions.xlsx | correct_fx_forward.json   | transaction_time | K20190805/95092/GBP | 2019-01-01 00:00:00+00:00 |
