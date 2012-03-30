module ExpectationHelper
  def expect_sequence(subject, chain, name)
    sequence_instance = sequence(name)

    chain.each do |step|
    	expectation = subject.expects(step[:method])

    	expectation.with(step[:param]) if step[:param]

    	expectation.returns(step[:return]).in_sequence(sequence_instance)
    end
  end
end
