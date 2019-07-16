Feature: AutoscaleJob - Integration Test
  Integration tests for AutoscaleJob

  Scenario Outline: Simple test
    Given that the current node count is <nodeCountBefore>
    And the current load is <load>
    Then the revised number of nodes should be <nodeCountAfter>

    Examples: Simple test
      | nodeCountBefore | load | nodeCountAfter |
      | 100             | 0.7  | 88             |
      | 100             | 0.75 | 94             |
      | 90              | 0.6  | 68             |
      | 20              | 0.7  | 18             |

  Scenario Outline: Test Disk Constraints
    Given that the current node count is <nodeCountBefore>
    And the current disk utilization is <diskUtilization>
    And the current load is <load>
    Then the revised number of nodes should be <nodeCountAfter>

    Examples: Test Disk Constraints
      | nodeCountBefore | diskUtilization | load | nodeCountAfter |
      | 100             | 0.8             | 0.4  | 115            |
      | 100             | 0.6             | 0.4  | 86             |
      | 100             | 0.6             | 0.72 | 90             |

  Scenario: Test Resize
    Given that the current node count is 100
    And the current load is 0.6
    Then the revised number of nodes should be 75

  Scenario: Test Huge Resize on Overload
    Given that the current node count is 100
    And the current load is 0.95
    Then the revised number of nodes should be 200

  Scenario: Job can't run twice.
    Given that the current node count is 100
    When the job is executed 2 times
    Then a RuntimeException is expected.
    
    Scenario: Disk Constraint does not Override if Desired Nodes are already Enough
      Given that the current node count is 100
      When the overload step is empty for the cluster
      And the current disk utilization is 0.8
      And the current load is 0.96
      Then the revised number of nodes should be 120