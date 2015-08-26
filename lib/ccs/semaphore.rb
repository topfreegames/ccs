require 'monitor'

module CCS
  class Semaphore
    def initialize(max = 100)
      @max = max
      @current = 0
      @con = Celluloid::Condition.new
      CCS.debug("initialize semaphore current=#{@current} max=#{@max}")
    end

    def take
      CCS.debug("Take semaphore return=#{@current == @max}, current=#{@current + 1 if @current != @max} max=#{@max}")
      return if @con.wait if @current == @max
      @current += 1
    end

    def release
      CCS.debug("Release semaphore signal=#{@current == @max}, current=#{@current - 1} max=#{@max}")
      @con.signal(false) if @current == @max
      @current -= 1
    end

    def interrupt
      CCS.debug("Interrupt semaphore signal=true, current=#{@current} max=#{@max}")
      @con.signal(true)
    end
  end
end
