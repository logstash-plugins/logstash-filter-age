# encoding: utf-8

require_relative '../spec_helper'

describe LogStash::Filters::Age do
  before do
    allow(Time).to receive(:now).and_return(now)
  end
  let(:now) { Time.mktime(2017, 1, 1) }

  context 'with default configuration' do
    let(:config) { 'filter { age {} }' }

    sample('message' => 'Hello World') do
      expect(subject.get('[@metadata][age]')).to eq(now.to_f - subject.timestamp.to_f)
    end
  end

  context 'with target' do
    let(:config) { "filter { age { target => '#{target}' } }" }
    let(:target) { 'age' }

    sample('message' => 'Hello World') do
      expect(subject.get(target)).to eq(now.to_f - subject.timestamp.to_f)
    end
  end
end
