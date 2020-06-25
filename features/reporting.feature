# Created by alex at 28/10/2019

#Feature: Generate useful reports for the back office

#  Scenario: Reyl cash reconciliation

#    Given some transactions and cash movements in the database
#    When the cash reconciliation is calculated for Reyl as of "2019-04-01"
#    Then all the rows should be ok


#  Scenario: Calculate trade summary

#    Given legacy Reyl data in the database
#    When the trade summary is calculated
#    Then the trades should have net pnls (gbp) of "[995, 2051, 3078]"

#  @working
#  Scenario Outline: Calculate Exante Cash Reconciliation

#    Given legacy Exante data in the database
#    When the cash reconciliation is calculated for Exante as of "<as_of_date>"
#    Then all the rows should be ok

#    Examples:
#    | as_of_date |
#    | 2019-09-16 |
#    | 2019-10-02 |
#    | 2019-10-16 |
#    | 2019-10-28 |
