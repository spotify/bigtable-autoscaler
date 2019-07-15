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
