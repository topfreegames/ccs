require 'monitor'

module CCS
  class Semaphore
    def initialize(max = 100)
      CCS.debug("initialize semaphore current=#{@current}")
      @max = max
      @current = 0
      @con = Celluloid::Condition.new
    end

    def take
      ret = @con.wait if @current == @max
      CCS.debug("Take semaphore ret=#{ret}, current=#{@current + 1}")

      return if ret
      @current += 1
    end

    def release
      signal = @current == @max
      CCS.debug("Release semaphore signal=#{!signal}, current=#{@current - 1}")
      
      @con.signal(false) if signal
      @current -= 1
    end

    def interrupt
      CCS.debug("Interrupt semaphore current=#{@current}")
      @con.signal(true)
    end
  end
end
