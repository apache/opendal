# frozen_string_literal: true

module OpenDAL
  class IO
    # Reads all lines from the stream into an array.
    # Raises `EOFError` when the end of the file is reached.
    def readlines
      results = []

      loop do
        results << readline
      rescue EOFError
        break
      end

      results
    end

    # Rewinds the stream to the beginning.
    def rewind
      seek(0, ::IO::SEEK_SET)
    end

    # Sets the file position to `new_position`.
    def pos=(new_position)
      seek(new_position, ::IO::SEEK_SET)
    end

    alias_method :pos, :tell

    # Checks if the stream is at the end of the file.
    def eof
      position = tell
      seek(0, ::IO::SEEK_END)
      tell == position
    end

    alias_method :eof?, :eof

    # Returns the total length of the stream.
    def length
      current_position = tell
      seek(0, ::IO::SEEK_END)
      tell.tap { self.pos = current_position }
    end

    alias_method :size, :length
  end
end
