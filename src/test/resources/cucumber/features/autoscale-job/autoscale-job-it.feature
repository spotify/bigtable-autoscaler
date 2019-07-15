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
      
      