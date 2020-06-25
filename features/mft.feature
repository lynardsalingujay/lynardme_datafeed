# Created by alex at 20/08/2019
Feature: MFT calculations
  # Enter feature description here

  Scenario Outline: Signals
    Given an index called "<index>"
    When I calculate the signal
    Then it is equal to "<signal>"

    Examples: Common indexes
      | index | signal |
      | SPX   | 0.005  |

  Scenario: Thresholds
    Given a sim result in the database
    When I calculate the thresholds using VIX
    Then the threshold table is correctly set