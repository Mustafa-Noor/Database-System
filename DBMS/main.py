import os
import time
from parser import parse_command
from user_manager import register, sign_in
from utils import clear_screen



def print_header():
    print("\n\n")
    print("=" * 50)
    print("  WELCOME TO LA VIDA DATABASE  ".center(50))
    print("=" * 50)
    

def startMenu():
    while True:
        print_header()
        print("\n1️. Register\n2️. Sign In\n3️. Exit")
        choice = input("🔹 Enter choice> ")

        if choice == "1":
            username = input("Enter username: ")
            password = input("Enter password: ")
            register(username, password)

        elif choice == "2":
            username = input("Enter username: ")
            password = input("Enter password: ")
            if(sign_in(username, password)):
                main()

        elif choice == "3":
            print("\n Goodbye! Thanks for using La Vida Database.")
            break

        else:
            print("\n Invalid choice, please try again!")

def main():
    clear_screen()
    while True:
        command = input("> ").strip()
        if command.upper() == "EXIT":
            print("Goodbye!")
            break
        parse_command(command)

if __name__ == "__main__":
    startMenu()
