Feature: AutoscaleJob - Integration Test
  Integration tests for AutoscaleJob

  Scenario Outline: Simple test
    Given that the current node count is <nodeCountBefore>
    When the current load is <load>
    And the job is executed 1 times
    Then the revised number of nodes should be <nodeCountAfter>

    Examples: Simple test
      | nodeCountBefore | load | nodeCountAfter |
      | 100             | 0.7  | 88             |
      | 100             | 0.75 | 94             |
      | 90              | 0.6  | 68             |
      | 20              | 0.7  | 18             |

  Scenario Outline: Test Disk Constraints
    Given that the current node count is <nodeCountBefore>
    When the current disk utilization is <diskUtilization>
    And the current load is <load>
    And the job is executed 1 times
    Then the revised number of nodes should be <nodeCountAfter>

    Examples: Test Disk Constraints
      | nodeCountBefore | diskUtilization | load | nodeCountAfter |
      | 100             | 0.8             | 0.4  | 115            |
      | 100             | 0.6             | 0.4  | 86             |
      | 100             | 0.6             | 0.72 | 90             |

  Scenario: Test Resize
    Given that the current node count is 100
    When the current load is 0.6
    And the job is executed 1 times
    Then the revised number of nodes should be 75

  Scenario: Test Huge Resize on Overload
    Given that the current node count is 100
    When the current load is 0.95
    And the job is executed 1 times
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
    And the job is executed 1 times
    Then the revised number of nodes should be 120

  Scenario: Testing Upper Bound Limit
    Given that the current node count is 480
    And the maximum number of nodes of 500
    When a new registry is created
    And a job is set
    And the current load is 0.9
    And the job is executed 1 times
    And the metric is created with filter overridden-desired-node-count
    Then the metrics size should be 1
    And the following should match:
      | 500 | target-nodes  |
      | 540 | desired-nodes |
    And the default values should match
    And the revised number of nodes should be 500

  Scenario: Testing Lower Bound Limit
    Given that the current node count is 7
    And the minimum number of nodes of 6
    When a new registry is created
    And a job is set
    And the current load is 0.0001
    And the job is executed 1 times
    And the metric is created with filter overridden-desired-node-count
    Then the metrics size should be 2
    And the following should match:
      | 6 | target-nodes  |
      | 5 | desired-nodes |
    And the default values should match
    And the revised number of nodes should be 6

  Scenario: Exponential Backoff After Consecutive Failures
    Given that the cluster had 5 failures
    Then the job should do an exponential backoff after 300 seconds
    But the job should not do an exponential backoff after 1000 seconds

  Scenario: No Exponential Backoff After Success
    Given that the cluster had 0 failures
    Then the job should not do an exponential backoff after 50 seconds

  Scenario: We don't resize too fast
    Given that the current node count is 100
    When the current load is 0.3
    And the job is executed 1 times
    Then the revised number of nodes should be 70

  Scenario: We Resize if Size Constraints are not Met
    Given that the cluster have a load delta of 10
    And the minimum number of nodes of 6
    And that the current node count is 6
    When a job is set
    And the current load is 0.1
    
    