import sys


class ProgressBar:
    def __init__(self, _min=0, _max=10, width=12):
        # Set base parameters
        self.min = _min
        self.max = _max
        self.span = _max - _min
        self.width = width

        # When progress == max, we are 100% done
        self.progress = 0

        # Build the progress bar string and prepare for drawing
        self.old_bar = ''
        self.progress_bar = "[]"
        self.update_progress(0)

    def update_progress(self, progress=0):
        if progress < self.min:
            progress = self.min
        if progress > self.max:
            progress = self.max
        self.progress = progress
        
        # Figure out how many hash bars the percentage should be
        done = float(self.progress - self.min) / float(self.span)
        hashes = int(round(done * (self.width - 2)))

        # build a progress bar with hashes and spaces
        self.progress_bar = '[' + \
                            '#' * hashes + \
                            ' ' * (self.width - hashes - 2) + \
                            ']'

        # Slice the percentage into the bar, roughly centered
        place = int(self.width / 2 - 1)
        percent = str(int(done * 100.0)) + "%"
        self.progress_bar = self.progress_bar[0:place] + \
                            percent + \
                            self.progress_bar[place + len(percent):]

        # Draw as needed
        self.draw()
    
    def draw(self):
        if self.progress_bar != self.old_bar:
            self.old_bar = self.progress_bar
            sys.stderr.write(self.progress_bar + '\r')
            sys.stderr.flush()

    def __str__(self):
        return str(self.progress_bar)
