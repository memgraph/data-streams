from argparse import ArgumentParser
from github import Github
from tqdm import tqdm
import csv
import dependency_graph


def parse_args():
    """
    Parse command line arguments.
    """
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--token", default="", type=str, help="Host address.")
    parser.add_argument("--repo", default="networkx/networkx",
                        type=str, help="Root repository for the network.")
    parser.add_argument(
        "--csv-output",
        default="../data/github-network.csv",
        help="Name of the CSV file.",
    )
    parser.add_argument(
        "--all",
        default=True,
        action="store_true",
        help="Generate the whole network."
    )
    parser.add_argument("--dependents", default=False, action="store_true",
                        help="Include dependents in the network.")
    parser.add_argument("--contributors", default=False, action="store_true",
                        help="Include contributors in the network.")
    parser.add_argument("--followers", default=True, action="store_true",
                        help="Include followers in the network.")
    parser.add_argument("--following", default=True, action="store_true",
                        help="Include following in the network.")
    print(__doc__)
    return parser.parse_args()


args = parse_args()
g = Github("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")


def get_contributors(repo):
    contributors = repo.get_contributors()
    number_of_contributors = contributors.totalCount
    print('Number of contributors:', number_of_contributors)
    return contributors


def get_followers(author):
    followers = author.get_followers()
    followers_names = []
    for follower in followers:
        followers_names.append(follower.login)
    return followers_names


def get_following(author):
    following = author.get_following()
    following_names = []
    for follows in following:
        following_names.append(follows.login)
    return following_names


def get_commits(repo):
    commits = repo.get_commits()
    number_of_commits = commits.totalCount
    print('Number of commits:', number_of_commits)
    return commits


def get_dependents(repo_name):
    dependents = dependency_graph.get_dependents(repo_name)
    number_of_dependents = len(dependents)
    print('Number of dependents:', number_of_dependents)
    return dependents


def scrape_data(repositories):
    for repo in repositories:
        repo_name = repo.full_name
        print(repo_name)

        if (args.dependents):
            dependents = get_dependents(repo_name)

        if (args.contributors):
            contributors = get_contributors(repo)

        fieldnames = ['commit', 'author', 'followers', 'following']
        with open(args.csv_output, 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            commits = get_commits(repo)
            for commit in tqdm(commits):
                author = commit.author

                followers = []
                if (args.all or args.followers):
                    followers = get_followers(author)

                following = []
                if (args.all or args.following):
                    following = get_following(author)

                writer.writerow({'commit': commit.sha,
                                'author': author.login,
                                 'followers': followers,
                                 'following': following})


def main():
    repositories = []
    #repositories = g.search_repositories("q=language:python", "stars", "desc")
    repositories.append(g.get_repo(args.repo))
    scrape_data(repositories)


if __name__ == "__main__":
    main()
