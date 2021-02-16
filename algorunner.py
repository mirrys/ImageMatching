import argparse
import papermill as pm
import os

# Todo: find a more accurate way to get dblist.
all_languages = languages = ['enwiki', 'arwiki', 'kowiki', 'cswiki', 'viwiki', 'frwiki', 'fawiki', 'ptwiki',
                             'ruwiki', 'trwiki', 'plwiki', 'hewiki', 'svwiki', 'ukwiki', 'huwiki', 'hywiki',
                             'srwiki', 'euwiki', 'arzwiki', 'cebwiki', 'dewiki', 'bnwiki']


class AlgoRunner(object):

    def __init__(self, snapshot, languages, output_dir):
        """
        :param str languages: A list of the languages separated by a comma to run against the algorithm.
        :param str snapshot: Snapshot date
        :param str output_dir: Directory to place output .ipynb and .tsv files
        """
        self.snapshot = snapshot
        self.languages = languages.split(',')
        self.output_dir = output_dir
        print(f'Initializing with snapshot={self.snapshot} languages={self.languages} output_dir={self.output_dir}')

    def run(self):
        if len(self.languages) == 1 and self.languages[0] == 'All':
            self.execute_papermill(all_languages)
        else:
            self.execute_papermill(self.languages)

    def execute_papermill(self, languages):
        """
        Executes jupyter notebook

        :param list languages: List of languages to run against the algorithm
        """
        print(f'Starting to execute the algorithm for the following languages: {languages}')

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        for language in languages:
            pm.execute_notebook(
                'algorithm.ipynb',
                self.output_dir + '/' + language + '_' + self.snapshot + '.ipynb',
                parameters=dict(language=language, snapshot=self.snapshot, output_dir=self.output_dir)
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Executes jupyter notebook with parameters. ' +
                                                 'Ex: python3 algorunner.py 2020-12-28 hywiki Output')
    parser.add_argument('snapshot', help='Full snapshot date. Ex: 2020-12-28')
    parser.add_argument('languages', nargs='?', default='All',
                        help='Language(s) to execute. If more than one separate with comma. Ex: enwiki,kowiki,arwiki')
    parser.add_argument('output_dir', nargs='?', default='Output',
                        help='Directory to place output .ipynb and .tsv files. Defaults to: Output')

    args = parser.parse_args()
    runner = AlgoRunner(args.snapshot, args.languages, args.output_dir)

    runner.run()
