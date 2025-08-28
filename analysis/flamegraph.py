import sys
import csv
import json


from matplotlib.cbook import boxplot_stats


def box_and_whiskers(data):
    return boxplot_stats(data)


def merge_stack(stack, into):
    into["value"] += stack["value"]
    into["children"].append(stack)
    return into
    

def add_node(tree, row, index):
    path = row[1]
    duration = int(row[-1])
   
    parts = path.split("::")
    method = parts[0] + "." + parts[1]
    walk = [method, *parts[2:]]

    curr = tree
    for part in walk:
        if part not in curr:
            curr[part] = {}
        curr = curr[part]

    if 'value' not in curr:
        curr['value'] = []

    curr['value'].append(duration)

    return tree


def make_stack(key, node):
    if "value" in node:
        data = box_and_whiskers(node["value"])[0]
        value = int(data['whishi'])

        return {
            "name": key,
            "value": value,
            "children": [],
        }
    
    value = 0
    children = []

    for child, n in node.items():
        s = make_stack(child, n)
        value += s["value"]
        children.append(s)

    return {
        "name": key,
        "value": value,
        "children": children,
    }


def validate_stack(stack):
    if len(stack["children"]) == 0:
        return True
    ok = all(validate_stack(child) for child in stack["children"])
    ok = ok and stack["value"] == sum(child["value"] for child in stack["children"])
    return ok


def main():
    file = sys.argv[1]

    with open(file, 'r') as f:
        reader = csv.reader(f)
        rows = [row for row in reader]

    root = {
        "name": "root",
        "value": 0,
        "children": [],
    }

    tree = {}
    for i, row in enumerate(rows):
        tree = add_node(tree, row, i)

    stack = make_stack("root", tree)
    assert(validate_stack(stack))

    print(json.dumps(stack, indent=2))
     

if __name__ == "__main__":
    main()
