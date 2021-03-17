import argparse
import papermill as pm
import os


class DatasetMetricsRunner:

    def __init__(self, snapshot, output_dir):
        """
        :param str snapshot: Snapshot date
        :param str output_dir: Directory to place output .ipynb and .csv files
        """
        self.snapshot = snapshot
        self.output_dir = output_dir
        print(f'Initializing with snapshot={self.snapshot} output_dir={self.output_dir}')

    def run(self):
        """
        Executes jupyter notebook
        """

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        pm.execute_notebook(
            'Dataset_metrics.ipynb',
            self.output_dir + '/dataset_metrics_' + self.snapshot + '.ipynb',
            parameters=dict(snapshot=self.snapshot, output_dir=self.output_dir)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Executes jupyter notebook with parameters. ' +
                                                 'Ex: python3 dataset_metrics_runner.py 2021-01 Output')
    parser.add_argument('snapshot', help='snapshot date. Ex: 2021-01')

    parser.add_argument('output_dir', nargs='?', default='Output',
                        help='Directory to place output .ipynb and .csv files. Defaults to: Output')

    args = parser.parse_args()
    runner = DatasetMetricsRunner(args.snapshot, args.output_dir)

    runner.run()
