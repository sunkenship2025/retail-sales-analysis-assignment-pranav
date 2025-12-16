import math
import pytest
@pytest.mark.sqrt
def test_sqrt():
  num=25
  assert math.sqrt(num)==5
@pytest.mark.square
def testsquare():
  num=7
  assert(7 * 7)== 40
@pytest.mark.otherss
def tesequality():    #test isnt there so it didnt run
  assert 10==11


