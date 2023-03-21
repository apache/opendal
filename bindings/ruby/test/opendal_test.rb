require_relative "test_helper"
require_relative "../lib/opendal"

class OpenDALTest < Minitest::Test
  def test_it_distance
    assert_equal 1, distance([1, 1], [2, 1])
  end
end
