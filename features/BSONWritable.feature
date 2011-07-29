Feature:  BSONWritable

 Scenario: comparison  tests
    Given I have an empty BSONWritable
    When I compare it to itself
    Then the result should be 0
    When I deserialize it
    Then the new one should equal the old one
    When I add key "foo" and value bar of type String
    And I compare it to itself
    Then the result should be 0
    When I deserialize it
    Then the new one should equal the old one
    When I add key "foo2" and value [1,2,3] of type Array
    And I deserialize it
    Then the new one should equal the old one

  Scenario Outline:
    Given I have an empty BSONWritable
    When I add key "<keyname>" and value <value> of type <valuetype>
    And I compare it to itself
    Then the result should be 0
    When I deserialize it
    Then the new one should equal the old one

    Examples: keys and vals
      | keyname | value         | valuetype |
      | 1       | 2             | Int       |
      | abc     | adf           | String    |
      | thearr  | [1,2,"three"] | Array     |



