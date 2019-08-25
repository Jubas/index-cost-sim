# System imports
import sys

# Simulation imports
import Const
import NVTree
import ExtCP
import InvIdx
import ProgressBar
import IO


class Simulation:
    def __init__(self, setup):
        # Get the settings from the setup dictionary
        ## GENERIC SETTINGS
        self.Experiment = setup['Experiment']
        self.Setup = setup['Setup']
        self.IO_Costs = setup['IO_Costs']
        self.Output_Freq = setup['Output_Freq']
        self.Total_Inserts = setup['Total_Inserts']
        self.Write_IOtrace = setup['Write_Iotrace']
        self.Log_Name = setup['Log_Name']

        # Create and initialize the IO cost center for the simulation
        self.IO = IO.IO(self.Experiment,
                        setup
                        )
        self.IO.set_io_cost_param_array(self.IO_Costs)


        # Create the tree instance
        if self.Experiment == Const.NVTREE:
            self.Index = NVTree.NVTree(self.IO, setup)
        elif self.Experiment == Const.EXT_CP:
            self.Index = ExtCP.ExtCP(self.IO, setup)
        elif self.Experiment == Const.INV_IDX:
            self.Index = InvIdx.InvIdx(self.IO, setup)
        else:
            raise NotImplementedError("Add new simulation for undefined index type")

    # Start the simulation ... it creates the tree and starts inserting
    def simulate(self):
        # Initialize the progressbar for console
        p = ProgressBar.ProgressBar(0, self.Total_Inserts, 77)

        # Loop through all insertions
        inserted_descriptors = 0
        while inserted_descriptors < self.Total_Inserts:
            # Write IO stats regularly as dictated by parameters
            if inserted_descriptors % self.Output_Freq == 0:
                # Update the progress bar
                p.update_progress(inserted_descriptors)
                # Print IO stats and tree stats
                out_stats = self.IO.get_io_stats()
                self.print_stats(inserted_descriptors, out_stats)
                self.print_stats(inserted_descriptors,
                                 self.Index.get_index_stats())
                # Make sure the stuff is printed to disk
                sys.stdout.flush()

            # Insert the descriptors, one at a time
            self.Index.insert()
            inserted_descriptors += 1

        # Get the final IO stats and print them to console
        p.update_progress(inserted_descriptors)

        #flush OS buffer (if any)
        self.Index.clear_osbuf()

        if self.Write_IOtrace == True:
            self.print_ioqueue(self.IO.IODepthDict)

        out_stats = self.IO.get_io_stats()
        self.print_stats(inserted_descriptors, out_stats)
        self.print_stats(inserted_descriptors,
                         self.Index.get_index_stats())

    def print_ioqueue(self, queue_dict):
        with open(self.Log_Name, 'w') as f:
            f.write("IO_type\t")
            f.write("\t".join(Const.IO_DEPTHS))
            f.write("\n")
            for idx, x in enumerate(queue_dict.values()):
                f.write("%s\t" % (Const.L_IO_TYPE_NAMES[idx]))
                f.write("\t".join(str(item) for item in x))
                f.write("\n")

    # Helper function for formatting the IO statistics
    def print_stats(self, inserted, stat_list):
        stat_string = "%s\t%s\t%f\t%s\n"
        for l in stat_list:
            sys.stdout.write(stat_string % (self.Experiment,
                                            self.Setup,
                                            inserted / 1000000.0,
                                            l))


