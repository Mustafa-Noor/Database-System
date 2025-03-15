import os
def wait_for_keypress():
    input("\n🔹 Press Enter to continue...")
    clear_screen()


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')