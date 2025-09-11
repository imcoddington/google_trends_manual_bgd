import os

topics = [
    "Demographic",
    "Economic",
    "Environmental",
    "Infrastructure",
    "Insecurities - Health, Food, Ph",
    "Other words",
    "Political & LawsPolicy",
    "Relief",
    "Social"
]

divisions = [
    "Barisal Division",
    "Chittagong Division",
    "Dhaka Division",
    "Khulna Division",
    "Rajshahi Division",
    "Rangpur Division",
    "Sylhet Division"
]

languages = ["English", "Bengali"]

def get_dir(division, language, topic):
    return f"./data/{division}_{language}/{topic}/"

if __name__ == "__main__":
    for division in divisions:
        for language in languages:
            for topic in topics:
                dir_path = get_dir(division, language, topic)
                os.makedirs(dir_path, exist_ok=True)
                print(f"Created directory: {dir_path}")
                #add a placeholder file to ensure directory is listed in git
                placeholder_file = os.path.join(dir_path, ".placeholder")
                with open(placeholder_file, "w") as f:
                    pass
