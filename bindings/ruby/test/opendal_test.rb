require_relative "test_helper"

class OpenDALTest < Minitest::Test
  def test_it_distance
    assert_equal 1, distance([1, 1], [2, 1])
  end
end
