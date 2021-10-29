import requests
from bs4 import BeautifulSoup


dependents_list = []


def get_dependents(repo):
    page_num = 3000
    url = 'https://github.com/{}/network/dependents'.format(repo)

    for i in range(page_num):
        print("GET " + url)
        r = requests.get(url)
        print(r)
        soup = BeautifulSoup(r.content, "html.parser")

        dependents_exist = soup.find('h3', {"data-view-component": "true"})
        if(dependents_exist and dependents_exist.text == "We haven’t found any dependents for this repository yet."):
            return {}

        data = [
            "{}/{}".format(
                t.find('a', {"data-repository-hovercards-enabled": ""}).text,
                t.find('a', {"data-hovercard-type": "repository"}).text
            )
            for t in soup.findAll("div", {"class": "Box-row"})
        ]
        dependents_list.extend(data)

        next_url = soup.find(
            "div", {"class": "paginate-container"})
        next_disabled = soup.find(
            "button", {"disabled": "disabled"})
        if(not next_url or next_disabled):
            return dependents_list

        url = next_url.find('a')["href"]
    return dependents_list


def main():
    dependents = get_dependents("memgraph/pymgclient")
    print(len(dependents))


if __name__ == "__main__":
    main()
